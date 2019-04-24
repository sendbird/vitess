#!/usr/bin/env python
#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import socket
import time
import unittest

import MySQLdb as db

import base_sharding
import environment
import tablet
import utils

from vtproto import topodata_pb2
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client

# source keyspace, with 4 tables
source_master = tablet.Tablet()
source_replica = tablet.Tablet()
source_rdonly = tablet.Tablet()

# destination keyspace, with just two tables
destination_master = tablet.Tablet()
destination_replica = tablet.Tablet()
destination_rdonly = tablet.Tablet()

all_tablets = [source_master, source_replica, source_rdonly,
               destination_master, destination_replica, destination_rdonly]

vschema = {
  'source_keyspace': '''{
    "tables": {
      "moving1": {},
      "moving2": {},
      "extra1": {},
      "extra2": {}
    }
  }''',
  'destination_keyspace': '''{
    "tables": {
      "moving1": {},
      "moving2": {}
    }
  }''',
}


def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [t.init_mysql(use_rbr=base_sharding.use_rbr)
                   for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  if utils.vtgate:
    utils.vtgate.kill()
  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()


class TestVerticalSplit(unittest.TestCase, base_sharding.BaseShardingTest):

  def setUp(self):
    self.insert_index = 0

    self._init_keyspaces_and_tablets()
    utils.apply_vschema(vschema)
    utils.VtGate(mysql_server=True).start(cache_ttl='0s', tablets=[
        source_master, source_replica, source_rdonly,
        destination_master, destination_replica, destination_rdonly])

    utils.vtgate.wait_for_endpoints(
        '%s.%s.master' % ('source_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.replica' % ('source_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.rdonly' % ('source_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.master' % ('destination_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.replica' % ('destination_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.rdonly' % ('destination_keyspace', '0'),
        1)

    # create the schema on the source keyspace, add some values
    self._insert_initial_values()

  def tearDown(self):
    # kill everything
    tablet.kill_tablets([source_master, source_replica, source_rdonly,
                         destination_master, destination_replica,
                         destination_rdonly])
    utils.vtgate.kill()

  def _init_keyspaces_and_tablets(self):
    utils.run_vtctl(['CreateKeyspace', 'source_keyspace'])
    utils.run_vtctl(['CreateKeyspace', 'destination_keyspace'])

    source_master.init_tablet(
        'replica',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=0)
    source_replica.init_tablet(
        'replica',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=1)
    source_rdonly.init_tablet(
        'rdonly',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=2)
    destination_master.init_tablet(
        'replica',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=0)
    destination_replica.init_tablet(
        'replica',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=1)
    destination_rdonly.init_tablet(
        'rdonly',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=2)

    utils.run_vtctl(
        ['RebuildKeyspaceGraph', 'source_keyspace'], auto_log=True)
    utils.run_vtctl(
        ['RebuildKeyspaceGraph', 'destination_keyspace'], auto_log=True)

    self._create_source_schema()

    for t in [source_master, source_replica,
              destination_master, destination_replica]:
      t.start_vttablet(wait_for_state=None)
    for t in [source_rdonly, destination_rdonly]:
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets
    master_tablets = [source_master, destination_master]
    replica_tablets = [
        source_replica, source_rdonly,
        destination_replica, destination_rdonly]
    for t in master_tablets + replica_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work (we use health check, fix their types)
    utils.run_vtctl(['InitShardMaster', '-force', 'source_keyspace/0',
                     source_master.tablet_alias], auto_log=True)
    source_master.tablet_type = 'master'
    utils.run_vtctl(['InitShardMaster', '-force', 'destination_keyspace/0',
                     destination_master.tablet_alias], auto_log=True)
    destination_master.tablet_type = 'master'

    for t in [source_replica, destination_replica]:
      utils.wait_for_tablet_type(t.tablet_alias, 'replica')
    for t in [source_rdonly, destination_rdonly]:
      utils.wait_for_tablet_type(t.tablet_alias, 'rdonly')

    for t in master_tablets + replica_tablets:
      t.wait_for_vttablet_state('SERVING')

  def _create_source_schema(self):
    create_table_template = '''create table %s(
id bigint not null,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''

    for t in [source_master, source_replica, source_rdonly]:
      t.create_db('vt_source_keyspace')
      for n in ['moving1', 'moving2', 'staying1', 'staying2']:
        t.mquery(source_master.dbname, create_table_template % (n))

    for t in [destination_master, destination_replica, destination_rdonly]:
      t.create_db('vt_destination_keyspace')

  def _insert_initial_values(self):
    self.moving1_first = self._insert_values('moving1', 100)
    self.moving2_first = self._insert_values('moving2', 100)
    staying1_first = self._insert_values('staying1', 100)
    staying2_first = self._insert_values('staying2', 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving1',
                       self.moving1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving2',
                       self.moving2_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying1',
                       staying1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying2',
                       staying2_first, 100)

  # insert some values in the source master db, return the first id used
  def _insert_values(self, table, count):
    result = self.insert_index
    conn = db.connect(host=socket.gethostbyname('localhost'),
                      port=utils.vtgate.mysql_port)
    cursor = conn.cursor()
    for _ in xrange(count):
      cursor.execute('insert into source_keyspace.%s (id, msg) values(%d, "value %d")' % (
          table, self.insert_index, self.insert_index), {})
      self.insert_index += 1
    conn.close()
    return result

  def _check_values(self, t, dbname, table, first, count):
    logging.debug(
        'Checking %d values from %s/%s starting at %d', count, dbname,
        table, first)
    rows = t.mquery(
        dbname, 'select id, msg from %s where id>=%d order by id limit %d' %
        (table, first, count))
    self.assertEqual(count, len(rows), 'got wrong number of rows: %d != %d' %
                     (len(rows), count))
    for i in xrange(count):
      self.assertEqual(first + i, rows[i][0], 'invalid id[%d]: %d != %d' %
                       (i, first + i, rows[i][0]))
      self.assertEqual('value %d' % (first + i), rows[i][1],
                       'invalid msg[%d]: "value %d" != "%s"' %
                       (i, first + i, rows[i][1]))

  def _check_values_timeout(self, t, dbname, table, first, count,
                            timeout=30):
    while True:
      try:
        self._check_values(t, dbname, table, first, count)
        return
      except Exception:  # pylint: disable=broad-except
        timeout -= 1
        if timeout == 0:
          raise
        logging.debug('Sleeping for 1s waiting for data in %s/%s', dbname,
                      table)
        time.sleep(1)

  def _check_blacklisted_tables(self, t, expected):
    status = t.get_status()
    if expected:
      self.assertIn('BlacklistedTables', status)
    else:
      self.assertNotIn('BlacklistedTables', status)

    # check we can or cannot access the tables
    for table in ['moving1', 'moving2']:
      if expected:
        # table is blacklisted, should get the error
        _, stderr = utils.run_vtctl(['VtTabletExecute', '-json',
                                     t.tablet_alias,
                                     'select count(1) from %s' % table],
                                    expect_fail=True)
        self.assertIn(
            'disallowed due to rule: enforce blacklisted tables',
            stderr)
      else:
        # table is not blacklisted, should just work
        qr = t.execute('select count(1) from %s' % table)
        logging.debug('Got %s rows from table %s on tablet %s',
                      qr['rows'][0][0], table, t.tablet_alias)

  def _verify_resharding(self, from_keyspace, to_keyspace, state):
    rules = json.loads(utils.vtgate.get_vschema())['routing_rules']
    self.assertEqual(rules['%s.moving1' % to_keyspace],
                           ['%s.moving1' % from_keyspace])
    self.assertEqual(rules['moving1'], ['%s.moving1' % from_keyspace])
    self.assertEqual(len(rules), 4)
    shard_info = utils.run_vtctl(['GetShard', '%s/0' % to_keyspace],
                    auto_log=True)
    src = json.loads(shard_info[0])['source_shards'][0]
    self.assertEqual(src['keyspace'], from_keyspace)
    self.assertEqual(src['tables'], ['moving1', 'moving2'])
    if from_keyspace == 'source_keyspace':
      result = destination_master.mquery('_vt', 'select * from vreplication')
    else:
      result = source_master.mquery('_vt', 'select * from vreplication')
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0][2],
      'keyspace:"%s" shard:"0" filter:<rules:<match:"moving1" > '
      'rules:<match:"moving2" > > ' % from_keyspace)
    self.assertEqual(result[0][11], state)

  def test_vertical_split(self):
    utils.run_vtctl(['CopySchemaShard', '--tables',
                     'moving1,moving2',
                     source_rdonly.tablet_alias, 'destination_keyspace/0'],
                     auto_log=True)
    utils.run_vtctl(['VerticalSplitClone', 'source_keyspace',
                     'destination_keyspace',
                     'moving1,moving2'],
                     auto_log=True)

    # We should ideally wait for vreplication state to 'Running' and
    # replication lag to reach 0. But waiting 10s achieves the same
    # thing.
    time.sleep(10)

    self._verify_resharding('source_keyspace', 'destination_keyspace', 'Running')

    # check values are present
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving1',
                       self.moving1_first, 100)
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving2',
                       self.moving2_first, 100)

    # add values to source, make sure they're replicated
    moving1_first_add1 = self._insert_values('moving1', 100)
    _ = self._insert_values('staying1', 100)
    moving2_first_add1 = self._insert_values('moving2', 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving1', moving1_first_add1, 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving2', moving2_first_add1, 100)

    # use vtworker to compare the data
    logging.debug('Running vtworker VerticalSplitDiff')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        '--alsologtostderr',
                        'VerticalSplitDiff',
                        '--min_healthy_rdonly_tablets', '1',
                        'destination_keyspace/0'], auto_log=True)

    utils.pause('Good time to test vtworker for diffs')

    # serve rdonly from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'rdonly'],
                    auto_log=True)
    rules = json.loads(utils.vtgate.get_vschema())['routing_rules']
    self.assertEqual(rules['destination_keyspace.moving1@rdonly'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['source_keyspace.moving1@rdonly'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['moving1@rdonly'], ['destination_keyspace.moving1'])
    self.assertEqual(len(rules), 10)

    # then serve replica from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    rules = json.loads(utils.vtgate.get_vschema())['routing_rules']
    self.assertEqual(rules['destination_keyspace.moving1@replica'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['source_keyspace.moving1@replica'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['moving1@replica'], ['destination_keyspace.moving1'])
    self.assertEqual(len(rules), 16)

    # move replica back and forth
    utils.run_vtctl(['MigrateServedFrom', '-reverse',
                     'destination_keyspace/0', 'replica'], auto_log=True)
    rules = json.loads(utils.vtgate.get_vschema())['routing_rules']
    self.assertNotIn('destination_keyspace.moving1@replica', rules)
    self.assertNotIn('source_keyspace.moving1@replica', rules)
    self.assertNotIn('moving1@replica', rules)
    self.assertEqual(len(rules), 10)

    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    rules = json.loads(utils.vtgate.get_vschema())['routing_rules']
    self.assertEqual(rules['destination_keyspace.moving1@replica'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['source_keyspace.moving1@replica'],
                           ['destination_keyspace.moving1'])
    self.assertEqual(rules['moving1@replica'], ['destination_keyspace.moving1'])
    self.assertEqual(len(rules), 16)

    # Migrate master, and reverse replication
    utils.run_vtctl(['MigrateServedFrom', '-reverse_replication',
                     'destination_keyspace/0', 'master'],
                    auto_log=True)

    self._verify_resharding('destination_keyspace', 'source_keyspace', 'Running')
    self._check_blacklisted_tables(destination_master, False)
    self._check_blacklisted_tables(source_master, True)

    # insert more data into logical source. This will get redirected
    # to destination, which is now the new source.
    moving1_first_add1 = self._insert_values('moving1', 100)
    # Check to see that the source (new destination) got the data.
    self._check_values_timeout(source_master, 'vt_source_keyspace',
                               'moving1', moving1_first_add1, 100)

    # Migrate master back, in one step. But don't reverse.
    utils.run_vtctl(['MigrateServedFrom', 'source_keyspace/0', 'master'],
                    auto_log=True)

    self._verify_resharding('source_keyspace', 'destination_keyspace', 'Stopped')
    self._check_blacklisted_tables(destination_master, True)
    self._check_blacklisted_tables(source_master, False)

  def _assert_tablet_controls(self, expected_dbtypes):
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    self.assertEqual(len(shard_json['tablet_controls']), len(expected_dbtypes))

    expected_dbtypes_set = set(expected_dbtypes)
    for tc in shard_json['tablet_controls']:
      self.assertIn(tc['tablet_type'], expected_dbtypes_set)
      self.assertEqual(['moving1', 'moving2'], tc['blacklisted_tables'])
      expected_dbtypes_set.remove(tc['tablet_type'])
    self.assertEqual(0, len(expected_dbtypes_set),
                     'Not all expected db types were blacklisted')


if __name__ == '__main__':
  base_sharding.use_rbr = True
  utils.main()

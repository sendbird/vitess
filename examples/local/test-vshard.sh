# Check and kill old stuff
pkill -9 -fl vtdataro
rm -rf ~/go/vtdataroot/*
pgrep -fl vtdataro

export TOPO=etcd2

# Terminal1
./etcd-up.sh
./vtctld-up.sh
./vtgate-up.sh

# Terminal2
mysql -h 127.0.0.1 -P 15306

export SRC_KS=src_ks
export DST_KS=dst_ks
export SRC_TABLET_ID=test-100
export DST_TABLET_ID=test-200

# Terminal1
./lvtctl.sh CreateKeyspace $SRC_KS
KEYSPACE=$SRC_KS UID_BASE=100 ./vttablet-up.sh
sleep 15
./lvtctl.sh InitShardMaster -force $SRC_KS/0 $SRC_TABLET_ID
./lvtctl.sh ApplySchema -sql-file=../common/vsplit_init_tables.sql $SRC_KS

./lvtctl.sh GetSchema $SRC_TABLET_ID

# Steady load test for a while.
./vtclient \
    -server 127.0.0.1:15991 \
    -timeout 1h \
    -target '$SRC_KS:0' \
    -count 1000000 \
    -parallel 1 \
    -qps 100 \
    -max_sequence_id 1000000000 \
    -bind_variables '[ 1, "msg 12345" ]' \
    "INSERT INTO moving1 (time_created_ns,message,page) VALUES (:v1, :v2, :v3)"

# Errors at 2x200 qps:
# failed to execute DML
# E0617 14:52:53.026287    8033 error_recorder.go:57] FirstErrorRecorder: error[43]: Code: ALREADY_EXISTS
# vtgate: http://localhost:15001/: target: $SRC_KS.0.master, used tablet: $SRC_TABLET_ID (localhost): vttablet: Duplicate entry '1974' for key 'PRIMARY' (errno 1062) (sqlstate 23000) (CallerID: unsecure_grpc_client): Sql: "insert into moving1(time_created_ns, message, page) values (:v1, :v2, :v3)/* vtgate:: filtered_replication_unfriendly */", BindVars: {#maxLimit: "type:INT64 value:\"10001\" "v1: "type:INT64 value:\"1\" "v2: "type:VARCHAR value:\"msg 12345\" "v3: "type:INT64 value:\"1974\" "}

# failed to execute DML
# E0617 14:52:53.108063    8033 error_recorder.go:57] FirstErrorRecorder: error[44]: Code: ALREADY_EXISTS
# vtgate: http://localhost:15001/: target: $SRC_KS.0.master, used tablet: $SRC_TABLET_ID (localhost): vttablet: Duplicate entry '1979' for key 'PRIMARY' (errno 1062) (sqlstate 23000) (CallerID: unsecure_grpc_client): Sql: "insert into moving1(time_created_ns, message, page) values (:v1, :v2, :v3)/* vtgate:: filtered_replication_unfriendly */", BindVars: {#maxLimit: "type:INT64 value:\"10001\" "v1: "type:INT64 value:\"1\" "v2: "type:VARCHAR value:\"msg 12345\" "v3: "type:INT64 value:\"1979\" "}

./lvtctl.sh CreateKeyspace $DST_KS
./lvtctl.sh GetSrvKeyspaceNames test
KEYSPACE=$DST_KS UID_BASE=200 ./vttablet-up.sh
./lvtctl.sh InitShardMaster -force $DST_KS/0 $DST_TABLET_ID
./lvtctl.sh ApplySchema -sql-file=../common/vsplit_init_tables.sql $DST_KS
./lvtctl.sh GetSrvKeyspaceNames test
./lvtctl.sh GetSchema $SRC_TABLET_ID
./lvtctl.sh ApplyVSchema -vschema_file=../common/vsplit_src_vschema.json $SRC_KS
# FIXME: Shouldn't $DST_KS.staying1 not exist?
./lvtctl.sh ApplyVSchema -vschema_file=../common/vsplit_dst_vschema.json $DST_KS
./lvtctl.sh CopySchemaShard -tables=moving1,moving2 $SRC_KS/0 $DST_KS/0
./lvtctl.sh GetSchema $DST_TABLET_ID
./lvtctl.sh GetSchema $SRC_TABLET_ID
./lvtctl.sh GetSchema test-101
./lvtctl.sh GetSchema test-101
./lvtctl.sh GetSchema $SRC_TABLET_ID
./lvtctl.sh GetSchema $DST_TABLET_ID
./lvtctl.sh VerticalSplitClone $SRC_KS $DST_KS moving1,moving2
./lvtctl.sh GetSchema $DST_TABLET_ID
./lvtctl.sh GetSchema $DST_TABLET_ID
# Gives error when run second time.
./lvtctl.sh VerticalSplitClone $SRC_KS $DST_KS moving1,moving2
./lvtctl.sh GetVSchema $SRC_KS
./lvtctl.sh GetVSchema $DST_KS
./lvtctl.sh GetShard $DST_KS/0
./lvtctl.sh GetShard $SRC_KS/0
ls
git log test-tutorial
git diff 1632350ab1b3a677c82065ca065a5f4f017ab877^ 1632350ab1b3a677c82065ca065a5f4f017ab877
git cherry-pick 1632350ab1b3a677c82065ca065a5f4f017ab877
ls
./203a_vertical_split_diff.sh
./203a_vertical_split_diff.sh
curl http://localhost:15001/debug/vschema
./lvtctl.sh ShowResharding $DST_KS/0
./lvtctl.sh MigrateServedFrom $DST_KS/0 rdonly
curl http://localhost:15001/debug/vschema
./203a_vertical_split_diff.sh
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom $DST_KS/0 replica
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom $DST_KS/0 replica
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom -reverse $DST_KS/0 replica
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom -reverse $DST_KS/0 rdonly
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom $DST_KS/0 replica
curl http://localhost:15001/debug/vschema
./lvtctl.sh MigrateServedFrom -h
./lvtctl.sh MigrateServedFrom --reverse_replication $DST_KS/0 master

curl http://localhost:15001/debug/vschema
./lvtctl.sh GetShard $SRC_KS/0
./lvtctl.sh GetSchema $DST_TABLET_ID
./lvtctl.sh GetSchema $SRC_TABLET_ID
./203a_vertical_split_diff.sh
./lvtctl.sh GetTablet
./lvtctl.sh GetTablets
./lvtctl.sh ListTablets
./lvtctl.sh ListAllTablets
curl http://localhost:15100/debug/vschema
curl http://localhost:15100
pgrep -fl vtdataro
pgrep -fl vtdataro |grep vttab
curl http://localhost:15100
curl http://localhost:15200
curl http://localhost:15200/debug/status
./lvtctl.sh
./lvtctl.sh VtTabletExecute
./lvtctl.sh VtTabletExecute -h
./vtctld-down.sh
cd -
make build
cd -
./vtctld-up.sh
./vtctld-down.sh
./vtctld-up.sh
./lvtctl.sh VtTabletExecute -h
./lvtctl.sh VtTabletExecute -h
git diff
git checkout lvtctl.sh
./vtctld-down.sh
pgrep -fl vtdataro |grep vtctld
kill -9 40278
kill -9 40278
./vtctld-up.sh
pgrep -fl vtdataro |grep vtctld
./lvtctl.sh VtTabletExecute -h
git diff
git commit -am'enable query'
./lvtctl.sh VtTabletExecute -h
./lvtctl.sh VtTabletExecute $SRC_TABLET_ID 'select count(1) from moving1'
./lvtctl.sh VtTabletExecute $DST_TABLET_ID 'select count(1) from moving1'
./lvtctl.sh VtTabletExecute $DST_TABLET_ID 'select * from moving1'
./lvtctl.sh VtTabletExecute $SRC_TABLET_ID 'select * from moving1'
./lvtctl.sh VtTabletExecute $SRC_TABLET_ID 'select * from moving1' -json
./lvtctl.sh VtTabletExecute -json $SRC_TABLET_ID 'select * from moving1'
./lvtctl.sh VtTabletExecute -json $DST_TABLET_ID 'select * from moving1'
./lvtctl.sh VtTabletExecute -json $DST_TABLET_ID 'select * from moving1'
./lvtctl.sh VtTabletExecute -json $DST_TABLET_ID 'select * from moving1'
./lvtctl.sh
./lvtctl.sh ShowResharding
./lvtctl.sh ShowResharding $SRC_KS
./lvtctl.sh ShowResharding $SRC_KS/0
./lvtctl.sh ShowResharding $DST_KS/0
./lvtctl.sh MigrateServedFrom $SRC_KS/0 master
./lvtctl.sh ShowResharding $DST_KS/0
./lvtctl.sh ShowResharding $SRC_KS/0
curl http://localhost:15200/debug/status
curl http://localhost:15200/debug/status |grep Black
curl http://localhost:15100/debug/status |grep Black
./lvtctl.sh ShowResharding $SRC_KS/0
./lvtctl.sh ShowResharding $DST_KS/0
./lvtctl.sh CancelResharding
./lvtctl.sh CancelResharding -h
./lvtctl.sh CancelResharding $DST_KS/0
./lvtctl.sh ShowResharding $DST_KS/0
./lvtctl.sh ShowResharding $SRC_KS/0

# Start replication if not started, upon MigrateServedFrom <keyspace/shard> master
./lvtctl.sh VReplicationExec $DST_TABLET_ID 'select * from _vt.vreplication'
./lvtctl.sh VReplicationExec $DST_TABLET_ID 'update _vt.vreplication set state="Running" where id=4'

git diff
git checkout -b tj-ss-vsplit
git diff
git push
git push -u ps
git diff
git diff
./lvtctl.sh
./lvtctl.sh
./lvtctl.sh |grep Get
./lvtctl.sh GetRoutingRules -h
./lvtctl.sh |grep Sou
./lvtctl.sh |grep VRe

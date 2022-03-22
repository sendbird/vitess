source ./env.sh # Required so that "mysql" works from alias

set -x

echo "Current shard routing rules:"
vtctldclient  --server localhost:15999  GetShardRoutingRules

echo "Stop reverse replication of partial MoveTables"
vtctlclient Workflow customer.partial1_reverse stop

echo "Directly update table on migrated shard, just to show that customer:-80 is not updated"

command mysql -S $VTDATAROOT/vt_0000000500/mysql.sock -u vt_dba -e "update vt_customer2.customer set email = concat('routed.', email)"

echo "customer:-80 tablet not updated"
command mysql -S $VTDATAROOT/vt_0000000300/mysql.sock -u vt_dba -e "select * from vt_customer.customer"

echo "using shard targeting, shows that data is coming from customer2:-80 and NOT customer:-80"
mysql -e "use customer:-80; select customer_id, email from customer order by customer_id"

echo "errors due to deny list as expected since this query is not shard-targetted"
mysql -e "select customer_id, email from customer order by customer_id"

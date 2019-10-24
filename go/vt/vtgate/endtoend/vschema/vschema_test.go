/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	cluster        *vttest.LocalCluster
	vtParams       mysql.ConnParams
	mysqlParams    mysql.ConnParams
	grpcAddress    string
	tabletHostName = flag.String("tablet_hostname", "", "the tablet hostname")

	schema = `
create table vt_user (
	id bigint,
	name varchar(64),
	primary key (id)
) Engine=InnoDB;
	
create table main (
	id bigint,
	val varchar(128),
	primary key(id)
) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	flag.Parse()

	var cfg vttest.Config
	cfg.Topology = &vttestpb.VTTestTopology{
		Keyspaces: []*vttestpb.Keyspace{{
			Name: "ks",
			Shards: []*vttestpb.Shard{{
				Name: "80",
			}},
		}},
	}
	cfg.ExtraMyCnf = []string{path.Join(os.Getenv("VTTOP"), "config/mycnf/rbr.cnf")}
	if err := cfg.InitSchemas("ks", schema, nil); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.RemoveAll(cfg.SchemaDir)
		os.Exit(1)
	}
	defer os.RemoveAll(cfg.SchemaDir)

	cfg.TabletHostName = *tabletHostName

	// List of users authorized to execute vschema ddl operations
	cfg.ExtraArg = append(cfg.ExtraArg, "-vschema_ddl_authorized_users=%")

	cluster = &vttest.LocalCluster{
		Config: cfg,
	}
	if err := cluster.Setup(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		cluster.TearDown()
		os.Exit(1)
	}
	defer cluster.TearDown()

	vtParams = mysql.ConnParams{
		Host: "localhost",
		Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
	}
	mysqlParams = cluster.MySQLConnParams()
	grpcAddress = fmt.Sprintf("localhost:%d", cluster.Env.PortForProtocol("vtcombo", "grpc"))

	os.Exit(m.Run())
}

func TestVSchema(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Test the blank database with no vschema
	exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")

	qr := exec(t, conn, "select id, name from vt_user order by id")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`
	assert.Equal(t, want, got)

	qr = exec(t, conn, "delete from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

	// Test Blank VSCHEMA
	qr = exec(t, conn, "SHOW VSCHEMA TABLES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("dual")]]`
	assert.Equal(t, want, got)

	// Use the DDL to create an unsharded vschema and test again

	// Create VSchema
	exec(t, conn, "begin")
	exec(t, conn, "ALTER VSCHEMA ADD TABLE vt_user")
	exec(t, conn, "select * from  vt_user")
	exec(t, conn, "commit")

	// Select to force update VSCHEMA
	exec(t, conn, "begin")
	exec(t, conn, "ALTER VSCHEMA ADD TABLE main")
	exec(t, conn, "select * from  main")
	exec(t, conn, "commit")

	// Test Showing Tables
	qr = exec(t, conn, "SHOW VSCHEMA TABLES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("vt_user")]]`
	assert.Equal(t, want, got)

	// Test Showing Vindexes
	qr = exec(t, conn, "SHOW VSCHEMA VINDEXES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

	// Test DML operations
	exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`
	assert.Equal(t, want, got)

	qr = exec(t, conn, "delete from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

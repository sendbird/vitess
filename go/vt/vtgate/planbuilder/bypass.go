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

package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// cache shard name to vindexes.Keyspace for optimization. key is <source_keyspace>.<shard>
var shardRouteMap map[string]*vindexes.Keyspace

func init() {
	shardRouteMap = make(map[string]*vindexes.Keyspace)
}

func buildPlanForBypass(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}

	switch vschema.Destination().(type) {
	case key.DestinationExactKeyRange:
		if _, ok := stmt.(*sqlparser.Insert); ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "INSERT not supported when targeting a key range: %s", vschema.TargetString())
		}
	case key.DestinationShard:
		shard := string(vschema.Destination().(key.DestinationShard))
		targetKeyspace, err := getShardRoute(vschema, keyspace.Name, shard)
		if err != nil {
			return nil, err
		}
		if targetKeyspace != nil {
			log.Infof("Routing shard %s to from keyspace %s to keyspace %s", shard, keyspace.Name, targetKeyspace.Name) // todo: remove
			keyspace = targetKeyspace
		}
	}

	return &engine.Send{
		Keyspace:             keyspace,
		TargetDestination:    vschema.Destination(),
		Query:                sqlparser.String(stmt),
		IsDML:                sqlparser.IsDMLStatement(stmt),
		SingleShardOnly:      false,
		MultishardAutocommit: sqlparser.MultiShardAutocommitDirective(stmt),
	}, nil
}

func getShardRoute(vschema plancontext.VSchema, keyspace, shard string) (*vindexes.Keyspace, error) {
	shardRouteKey := fmt.Sprintf("%s.%s", keyspace, shard)
	if ks, ok := shardRouteMap[shardRouteKey]; ok {
		return ks, nil
	}
	targetKeyspaceName, err := vschema.FindRoutedShard(keyspace, shard)
	if err != nil {
		return nil, err
	}
	if targetKeyspaceName != keyspace {
		keyspaces, err := vschema.AllKeyspace()
		if err != nil {
			return nil, err
		}
		for _, ks := range keyspaces {
			if ks.Name == targetKeyspaceName {
				shardRouteMap[shardRouteKey] = ks // store route from source to target ks for this shard
				return ks, nil
			}
		}
	}
	return nil, nil
}

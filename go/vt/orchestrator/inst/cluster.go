/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"fmt"
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/kv"
)

func GetClusterPrimaryKVKey(clusterAlias string) string {
	return fmt.Sprintf("%s%s", config.Config.KVClusterPrimaryPrefix, clusterAlias)
}

func getClusterPrimaryKVPair(clusterAlias string, primaryKey *InstanceKey) *kv.KVPair {
	if clusterAlias == "" {
		return nil
	}
	if primaryKey == nil {
		return nil
	}
	return kv.NewKVPair(GetClusterPrimaryKVKey(clusterAlias), primaryKey.StringCode())
}

// GetClusterPrimaryKVPairs returns all KV pairs associated with a primary. This includes the
// full identity of the primary as well as a breakdown by hostname, port, ipv4, ipv6
func GetClusterPrimaryKVPairs(clusterAlias string, primaryKey *InstanceKey) (kvPairs [](*kv.KVPair)) {
	primaryKVPair := getClusterPrimaryKVPair(clusterAlias, primaryKey)
	if primaryKVPair == nil {
		return kvPairs
	}
	kvPairs = append(kvPairs, primaryKVPair)

	addPair := func(keySuffix, value string) {
		key := fmt.Sprintf("%s/%s", primaryKVPair.Key, keySuffix)
		kvPairs = append(kvPairs, kv.NewKVPair(key, value))
	}

	addPair("hostname", primaryKey.Hostname)
	addPair("port", fmt.Sprintf("%d", primaryKey.Port))
	if ipv4, ipv6, err := readHostnameIPs(primaryKey.Hostname); err == nil {
		addPair("ipv4", ipv4)
		addPair("ipv6", ipv6)
	}
	return kvPairs
}

// mappedClusterNameToAlias attempts to match a cluster with an alias based on
// configured ClusterNameToAlias map
func mappedClusterNameToAlias(clusterName string) string {
	for pattern, alias := range config.Config.ClusterNameToAlias {
		if pattern == "" {
			// sanity
			continue
		}
		if matched, _ := regexp.MatchString(pattern, clusterName); matched {
			return alias
		}
	}
	return ""
}

// ClusterInfo makes for a cluster status/info summary
type ClusterInfo struct {
	ClusterName                             string
	ClusterAlias                            string // Human friendly alias
	ClusterDomain                           string // CNAME/VIP/A-record/whatever of the primary of this cluster
	CountInstances                          uint
	HeuristicLag                            int64
	HasAutomatedPrimaryRecovery             bool
	HasAutomatedIntermediatePrimaryRecovery bool
}

// ReadRecoveryInfo
func (this *ClusterInfo) ReadRecoveryInfo() {
	this.HasAutomatedPrimaryRecovery = this.filtersMatchCluster(config.Config.RecoverPrimaryClusterFilters)
	this.HasAutomatedIntermediatePrimaryRecovery = this.filtersMatchCluster(config.Config.RecoverIntermediatePrimaryClusterFilters)
}

// filtersMatchCluster will see whether the given filters match the given cluster details
func (this *ClusterInfo) filtersMatchCluster(filters []string) bool {
	for _, filter := range filters {
		if filter == this.ClusterName {
			return true
		}
		if filter == this.ClusterAlias {
			return true
		}
		if strings.HasPrefix(filter, "alias=") {
			// Match by exact cluster alias name
			alias := strings.SplitN(filter, "=", 2)[1]
			if alias == this.ClusterAlias {
				return true
			}
		} else if strings.HasPrefix(filter, "alias~=") {
			// Match by cluster alias regex
			aliasPattern := strings.SplitN(filter, "~=", 2)[1]
			if matched, _ := regexp.MatchString(aliasPattern, this.ClusterAlias); matched {
				return true
			}
		} else if filter == "*" {
			return true
		} else if matched, _ := regexp.MatchString(filter, this.ClusterName); matched && filter != "" {
			return true
		}
	}
	return false
}

// ApplyClusterAlias updates the given clusterInfo's ClusterAlias property
func (this *ClusterInfo) ApplyClusterAlias() {
	if this.ClusterAlias != "" && this.ClusterAlias != this.ClusterName {
		// Already has an alias; abort
		return
	}
	if alias := mappedClusterNameToAlias(this.ClusterName); alias != "" {
		this.ClusterAlias = alias
	}
}

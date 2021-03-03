/*
Copyright 2020 The Vitess Authors.

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

package errors

import "errors"

var (
	// ErrAmbiguousTablet occurs when more than one tablet is found for a given
	// set of filter criteria.
	ErrAmbiguousTablet = errors.New("multiple tablets found")
	// ErrInvalidRequest occurs when a request is invalid for any reason.
	// For example, if mandatory parameters are undefined.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrNoTablet occurs when a tablet cannot be found for a given set of
	// filter criteria.
	ErrNoTablet = errors.New("no such tablet")
	// ErrUnsupportedCluster occurs when a cluster parameter is invalid.
	ErrUnsupportedCluster = errors.New("unsupported cluster(s)")
	// ErrNoSrvVSchema occurs when no SrvVSchema is found for a given keyspace.
	ErrNoSrvVSchema = errors.New("SrvVSchema not found")
)

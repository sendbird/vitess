package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetTablets implements the http wrapper for /tablets[?cluster=[&cluster=]].
func GetTablets(ctx context.Context, r Request, api *API) *JSONResponse {
	tablets, err := api.server.GetTablets(ctx, &vtadminpb.GetTabletsRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(tablets, err)
}

// GetTablet implements the http wrapper for /tablet/{tablet}[?cluster=[&cluster=]].
func GetTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	tablet, err := api.server.GetTablet(ctx, &vtadminpb.GetTabletRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(tablet, err)
}

func DeleteTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	deleted, err := api.server.DeleteTablet(ctx, &vtadminpb.DeleteTabletRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(deleted, err)
}

// PingTablet checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.
func PingTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	ping, err := api.server.PingTablet(ctx, &vtadminpb.PingTabletRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(ping, err)
}

// RefreshState reloads the tablet record on the specified tablet.
func RefreshState(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.RefreshState(ctx, &vtadminpb.RefreshStateRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// ReparentTablet reparents a tablet to the current primary in the shard. This
// only works if the current replica position matches the last known reparent
// action.
func ReparentTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.ReparentTablet(ctx, &vtadminpb.ReparentTabletRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// RunHealthCheck runs a healthcheck on the tablet and returns the result.
func RunHealthCheck(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.RunHealthCheck(ctx, &vtadminpb.RunHealthCheckRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// SetReadOnly sets the tablet to read only mode
func SetReadOnly(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.SetReadOnly(ctx, &vtadminpb.SetReadOnlyRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// SetReadWrite sets the tablet to read write mode
func SetReadWrite(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.SetReadWrite(ctx, &vtadminpb.SetReadWriteRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// StartReplication starts replication on the specified tablet.
func StartReplication(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.StartReplication(ctx, &vtadminpb.StartReplicationRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// StartReplication stops replication on the specified tablet.
func StopReplication(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	result, err := api.server.StopReplication(ctx, &vtadminpb.StopReplicationRequest{
		Alias:      vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

package actions

import (
	"fmt"
	"log/slog"
)

// Health checks the health of all replicas
func Health(ctx *Context) error {
	slog.Debug("checking health of all replicas")

	allHealthy := true
	for i, c := range ctx.Clients {
		health, err := c.Health(0)
		if err != nil {
			slog.Warn("replica unhealthy", "replica", i, "endpoint", c.BaseURL(), "error", err)
			allHealthy = false
			continue
		}

		slog.Debug("replica health", "replica", i, "endpoint", c.BaseURL(), "status", health.Status, "clusterStatus", health.ClusterStatus)
	}

	if !allHealthy {
		return fmt.Errorf("one or more replicas are unhealthy")
	}

	slog.Debug("all replicas healthy")
	return nil
}

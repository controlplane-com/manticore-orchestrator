package actions

import (
	"fmt"
	"log"
)

// Health checks the health of all replicas
func Health(ctx *Context) error {
	log.Println("Checking health of all replicas...")

	allHealthy := true
	for i, c := range ctx.Clients {
		health, err := c.Health()
		if err != nil {
			log.Printf("Replica %d (%s): UNHEALTHY - %v", i, c.BaseURL(), err)
			allHealthy = false
			continue
		}

		log.Printf("Replica %d (%s): %s, cluster: %s",
			i, c.BaseURL(), health.Status, health.ClusterStatus)
	}

	if !allHealthy {
		return fmt.Errorf("one or more replicas are unhealthy")
	}

	log.Println("All replicas healthy")
	return nil
}

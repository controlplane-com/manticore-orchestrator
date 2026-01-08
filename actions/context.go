package actions

import (
	"encoding/json"
	"os"

	"github.com/controlplane-com/manticore-orchestrator/client"
)

// Context holds the execution context for actions
type Context struct {
	Clients   []*client.AgentClient
	Dataset   string // e.g., "addresses"
	CSVPath   string // e.g., "addresses.csv"
	StateFile string
}

// State holds persistent state between orchestrator runs
type State struct {
	ActiveSlot string `json:"activeSlot"` // "a" or "b"
}

// LoadState loads state from file
func (c *Context) LoadState() (*State, error) {
	data, err := os.ReadFile(c.StateFile)
	if err != nil {
		if os.IsNotExist(err) {
			// Default state
			return &State{ActiveSlot: "a"}, nil
		}
		return nil, err
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// SaveState saves state to file
func (c *Context) SaveState(state *State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(c.StateFile, data, 0644)
}

// MainTableName returns the main table name for a given slot
func (c *Context) MainTableName(slot string) string {
	return c.Dataset + "_main_" + slot
}

// DeltaTableName returns the delta table name
func (c *Context) DeltaTableName() string {
	return c.Dataset + "_delta"
}

// DistributedTableName returns the distributed table name
func (c *Context) DistributedTableName() string {
	return c.Dataset
}

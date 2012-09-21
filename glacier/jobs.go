package glacier

import (
	"encoding/json"
)

type job struct {
	Type        string
	ArchiveId   string "json:,omitempty" // archive retrieval only
	Description string
	Format      string "json:,omitempty" // inventory retrieval only
	SNSTopic    string
}

func (c *Connection) InitiateRetrievalJob(vault, archive, topic string) error {
	return nil
}

func (c *Connection) InitiateInventoryJob(vault, description,
	topic string) error {
	j := job{Type: "inventory-retrieval", Description: description,
		SNSTopic: topic}
	_, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) DescribeJob() error {
	return nil
}

func (c *Connection) GetRetrievalJob(vault, job string, start, end uint) error {
	return nil
}

func (c *Connection) GetInventoryJob(vault, job string) error {
	return nil
}

func (c *Connection) ListJobs(vault string) error {
	return nil
}

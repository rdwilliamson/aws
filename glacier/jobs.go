package glacier

func (c *Connection) InitiateRetrievalJob(vault, archive, topic string) error {
	return nil
}

func (c *Connection) InitiateInventoryJob(vault string) error {
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

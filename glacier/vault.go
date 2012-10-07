package glacier

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/rdwilliamson/aws"
	"io/ioutil"
	"net/http"
	"time"
)

// A vault is a container for storing archives.
type Vault struct {
	CreationDate      time.Time
	LastInventoryDate time.Time
	NumberOfArchives  uint
	SizeInBytes       uint64
	VaultARN          string
	VaultName         string
}

// TODO write unmarshaler instead of using these structs
type vault struct {
	CreationDate      string
	LastInventoryDate *string
	NumberOfArchives  uint
	SizeInBytes       uint64
	VaultARN          string
	VaultName         string
}

type Notifications struct {
	Events   []string
	SNSTopic string
}

func (c *Connection) CreateVault(name string) error {
	request, err := http.NewRequest("PUT", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 201 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		err = response.Body.Close()
		if err != nil {
			return err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return &e
	}

	return nil
}

func (c *Connection) DeleteVault(name string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 204 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		err = response.Body.Close()
		if err != nil {
			return err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return &e
	}

	return nil
}

func (c *Connection) DescribeVault(name string) (*Vault, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = response.Body.Close()
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, &e
	}

	var v vault
	err = json.Unmarshal(body, &v)
	if err != nil {
		return nil, err
	}

	var result Vault
	var err1 error
	result.CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
	if err != nil {
		err1 = err
	}
	if v.LastInventoryDate != nil {
		result.LastInventoryDate, err = time.Parse(time.RFC3339, *v.LastInventoryDate)
		if err != nil && err1 == nil {
			err1 = err
		}
	}
	result.NumberOfArchives = v.NumberOfArchives
	result.SizeInBytes = v.SizeInBytes
	result.VaultARN = v.VaultARN
	result.VaultName = v.VaultName

	return &result, err1
}

func (c *Connection) ListVaults(marker string, limit int) ([]Vault, string, error) {
	if limit < 0 || limit > 1000 {
		// TODO return predeclared variable
		return nil, "", errors.New("limit must be 1 through 1000")
	}

	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults", nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	if limit != 0 {
		request.Header.Add("limit", "")
	}
	if marker != "" {
		request.Header.Add("marker", "")
	}

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, "", err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
	}
	err = response.Body.Close()
	if err != nil {
		return nil, "", err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, "", err
		}
		return nil, "", &e
	}

	var vaults struct {
		Marker    *string
		VaultList []vault
	}
	err = json.Unmarshal(body, &vaults)
	if err != nil {
		return nil, "", err
	}

	result := make([]Vault, len(vaults.VaultList))
	var err1 error
	for i, v := range vaults.VaultList {
		result[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil && err1 == nil {
			err1 = err
		}
		if v.LastInventoryDate != nil {
			result[i].LastInventoryDate, err = time.Parse(time.RFC3339, *v.LastInventoryDate)
			if err != nil && err1 == nil {
				err1 = err
			}
		}
		result[i].NumberOfArchives = v.NumberOfArchives
		result[i].SizeInBytes = v.SizeInBytes
		result[i].VaultARN = v.VaultARN
		result[i].VaultName = v.VaultName
	}

	var resultMarker string
	if vaults.Marker != nil {
		resultMarker = *vaults.Marker
	}

	return result, resultMarker, err1
}

func (c *Connection) SetVaultNotifications(name string, n *Notifications) error {
	body, err := json.Marshal(n)
	if err != nil {
		return err
	}
	bodyReader := bytes.NewReader(body)

	request, err := http.NewRequest("PUT", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", bodyReader)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, bodyReader, nil)

	bodyReader.Seek(0, 0)

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 204 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		err = response.Body.Close()
		if err != nil {
			return err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return &e
	}

	return nil
}

func (c *Connection) GetVaultNotifications(name string) (*Notifications, error) {
	var results Notifications

	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = response.Body.Close()
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, &e
	}

	err = json.Unmarshal(body, &results)
	if err != nil {
		return nil, err
	}

	return &results, nil
}

func (c *Connection) DeleteVaultNotifications(name string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 204 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		err = response.Body.Close()
		if err != nil {
			return err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return &e
	}

	return nil
}

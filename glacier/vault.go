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
type vaultsList struct {
	Marker    *string
	VaultList []vault
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

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return err
	}

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
		return e
	}

	return nil
}

func (c *Connection) DeleteVault(name string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return err
	}

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
		return e
	}

	return nil
}

func (c *Connection) DescribeVault(name string) (Vault, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return Vault{}, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return Vault{}, err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return Vault{}, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return Vault{}, err
	}
	err = response.Body.Close()
	if err != nil {
		return Vault{}, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return Vault{}, err
		}
		return Vault{}, e
	}

	var v vault
	err = json.Unmarshal(body, &v)
	if err != nil {
		return Vault{}, err
	}

	var result Vault
	result.CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
	if err != nil {
		return Vault{}, err
	}
	if v.LastInventoryDate != nil {
		result.LastInventoryDate, err =
			time.Parse(time.RFC3339, *v.LastInventoryDate)
		if err != nil {
			return Vault{}, err
		}
	}
	result.NumberOfArchives = v.NumberOfArchives
	result.SizeInBytes = v.SizeInBytes
	result.VaultARN = v.VaultARN
	result.VaultName = v.VaultName

	return result, nil
}

func (c *Connection) ListVaults(limit int, marker string) (string, []Vault, error) {
	if limit < 0 || limit > 1000 {
		// TODO return predeclared variable
		return "", nil, errors.New("limit must be 1 through 1000")
	}

	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults", nil)
	if err != nil {
		return "", nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	if limit != 0 {
		request.Header.Add("limit", "")
	}
	if marker != "" {
		request.Header.Add("marker", "")
	}

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return "", nil, err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return "", nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", nil, err
	}
	err = response.Body.Close()
	if err != nil {
		return "", nil, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return "", nil, err
		}
		return "", nil, e
	}

	var vaults vaultsList
	err = json.Unmarshal(body, &vaults)
	if err != nil {
		return "", nil, err
	}

	responseVaults := make([]Vault, len(vaults.VaultList))
	for i := range responseVaults {
		responseVaults[i].CreationDate, err = time.Parse(time.RFC3339, vaults.VaultList[i].CreationDate)
		if err != nil {
			return "", nil, err
		}
		if vaults.VaultList[i].LastInventoryDate != nil {
			responseVaults[i].LastInventoryDate, err = time.Parse(time.RFC3339, *vaults.VaultList[i].LastInventoryDate)
			if err != nil {
				return "", nil, err
			}
		}
		responseVaults[i].NumberOfArchives = vaults.VaultList[i].NumberOfArchives
		responseVaults[i].SizeInBytes = vaults.VaultList[i].SizeInBytes
		responseVaults[i].VaultARN = vaults.VaultList[i].VaultARN
		responseVaults[i].VaultName = vaults.VaultList[i].VaultName
	}

	var responseMarker string
	if vaults.Marker != nil {
		responseMarker = *vaults.Marker
	}

	return responseMarker, responseVaults, nil
}

func (c *Connection) SetVaultNotifications(name string, n Notifications) error {
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

	err = c.Signature.Sign(request, bodyReader, nil)
	if err != nil {
		return err
	}
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
		return e
	}

	return nil
}

func (c *Connection) GetVaultNotifications(name string) (Notifications, error) {
	var results Notifications

	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return results, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return results, err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return results, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return results, err
	}
	err = response.Body.Close()
	if err != nil {
		return results, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return results, err
		}
		return results, e
	}

	err = json.Unmarshal(body, &results)
	if err != nil {
		return results, err
	}

	return results, nil
}

func (c *Connection) DeleteVaultNotifications(name string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return err
	}

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
		return e
	}

	return nil
}

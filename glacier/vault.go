package glacier

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/rdwilliamson/aws"
)

// A vault is a container for storing archives. You can store an unlimited
// number of archives in a vault.
type Vault struct {
	CreationDate      time.Time
	LastInventoryDate time.Time
	NumberOfArchives  int
	SizeInBytes       int64
	VaultARN          string
	VaultName         string
}

// TODO write unmarshaler instead of using these structs
type vault struct {
	CreationDate      string
	LastInventoryDate *string
	NumberOfArchives  int
	SizeInBytes       int64
	VaultARN          string
	VaultName         string
}

// You can configure a vault to publish a notification for the following vault
// events:
// * ArchiveRetrievalCompleted: This event occurs when a job that was initiated
// for an archive retrieval is completed. The status of the completed job can be
// Succeeded or Failed.
// * InventoryRetrievalCompleted: This event occurs when a job that was
// initiated for an inventory retrieval is completed. The status of the
// completed job can be Succeeded or Failed.
type Notifications struct {
	Events   []string
	SNSTopic string
}

// This operation creates a new vault with the specified name. The name of the
// vault must be unique within a region for an AWS account.You can create up to
// 1,000 vaults per account.
//
// You must use the following guidelines when naming a vault.
// * Names can be between 1 and 255 characters long.
// * Allowed characters are a–z, A–Z, 0–9, '_' (underscore), '-' (hyphen), and
// '.' (period).
//
// This operation is idempotent, you can send the same request multiple times
// and it has no further effect after the first time Amazon Glacier creates the
// specified vault.
func (c *Connection) CreateVault(name string) error {
	// Build request.
	request, err := http.NewRequest("PUT", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return aws.ParseError(response)
	}

	// Parse success response.
	return nil
}

// This operation deletes a vault. Amazon Glacier will delete a vault only if
// there are no archives in the vault as per the last inventory and there have
// been no writes to the vault since the last inventory. If either of these
// conditions is not satisfied, the vault deletion fails (that is, the vault is
// not removed) and Amazon Glacier returns an error.
//
// This operation is idempotent.
func (c *Connection) DeleteVault(name string) error {
	// Build request.
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return aws.ParseError(response)
	}

	// Parse success response.
	return nil
}

// This operation returns information about a vault, including the vault Amazon
// Resource Name (ARN), the date the vault was created, the number of archives
// contained within the vault, and the total size of all the archives in the
// vault.The number of archives and their total size are as of the last vault
// inventory Amazon Glacier generated. Amazon Glacier generates vault
// inventories approximately daily. This means that if you add or remove an
// archive from a vault, and then immediately send a Describe Vault request, the
// response might not reflect the changes.
func (c *Connection) DescribeVault(name string) (*Vault, error) {
	// Build request.
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var v vault
	err = json.Unmarshal(body, &v)
	if err != nil {
		return nil, err
	}

	var result Vault
	result.CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
	if err != nil {
		return nil, err
	}
	if v.LastInventoryDate != nil {
		result.LastInventoryDate, err = time.Parse(time.RFC3339, *v.LastInventoryDate)
		if err != nil {
			return nil, err
		}
	}
	result.NumberOfArchives = v.NumberOfArchives
	result.SizeInBytes = v.SizeInBytes
	result.VaultARN = v.VaultARN
	result.VaultName = v.VaultName

	return &result, nil
}

// This operation lists all vaults owned by the calling user’s account. The list
// returned in the response is ASCII-sorted by vault name.
//
// By default, this operation returns up to 1,000 items. If there are more
// vaults to list, the marker field in the response body contains the vault
// Amazon Resource Name (ARN) at which to continue the list with a new List
// Vaults request; otherwise, the marker field is null. In your next List Vaults
// request you set the marker parameter to the value Amazon Glacier returned in
// the responses to your previous List Vaults request.You can also limit the
// number of vaults returned in the response by specifying the limit parameter
// in the request.
func (c *Connection) ListVaults(marker string, limit int) ([]Vault, string, error) {
	// Build request.
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

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, "", aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
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
	for i, v := range vaults.VaultList {
		result[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil {
			return nil, "", err
		}
		if v.LastInventoryDate != nil {
			result[i].LastInventoryDate, err = time.Parse(time.RFC3339, *v.LastInventoryDate)
			if err != nil {
				return nil, "", err
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

	return result, resultMarker, nil
}

// Retrieving an archive and a vault inventory are asynchronous operations in
// Amazon Glacier for which you must first initiate a job and wait for the job
// to complete before you can download the job output. Most Amazon Glacier jobs
// take about four hours to complete. So you can configure a vault to post a
// message to an Amazon Simple Notification Service (SNS) topic when these jobs
// complete. You can use this operation to set notification configuration on the
// vault.
//
// Amazon SNS topics must grant permission to the vault to be allowed to publish
// notifications to the topic.
//
// To configure vault notifications, send a request to the notification-
// configuration subresource of the vault. A notification configuration is
// specific to a vault; therefore, it is also referred to as a vault
// subresource.
func (c *Connection) SetVaultNotifications(name string, n *Notifications) error {
	// Build request.
	body, err := json.Marshal(n)
	if err != nil {
		return err
	}

	request, err := http.NewRequest("PUT", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, aws.MemoryPayload(body))

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return aws.ParseError(response)
	}

	// Parse success response.
	return nil
}

// This operation retrieves the notification-configuration subresource set on
// the vault. If notification configuration for a vault is not set, the
// operation returns a 404 Not Found error.
func (c *Connection) GetVaultNotifications(name string) (*Notifications, error) {
	// Build request.
	var results Notifications

	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &results)
	if err != nil {
		return nil, err
	}

	return &results, nil
}

// This operation deletes the notification configuration set for a vault. The
// operation is eventually consistent—that is, it might take some time for
// Amazon Glacier to completely disable the notifications, and you might still
// receive some notifications for a short time after you send the delete
// request.
func (c *Connection) DeleteVaultNotifications(name string) error {
	// Build request.
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+name+
		"/notification-configuration", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return aws.ParseError(response)
	}

	// Parse success response.
	return nil
}

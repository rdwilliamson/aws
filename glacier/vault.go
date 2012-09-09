package glacier

import (
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
	NumberOfArchives  int
	SizeInBytes       int // int64?
	VaultARN          string
	VaultName         string
}

// used for unmarshaling json
type vault struct {
	CreationDate      string
	LastInventoryDate *string
	NumberOfArchives  int
	SizeInBytes       int
	VaultARN          string
	VaultName         string
}

type vaultsList struct {
	Marker    *string
	VaultList []vault
}

func (gc *GlacierConnection) ListVaults(limit int, marker string) (string, []Vault, error) {
	if limit < 0 || limit > 1000 {
		return "", nil, errors.New("limit must be 1 through 1000")
	}

	request, err := http.NewRequest("GET",
		"https://"+aws.USEast.Glacier+"/-/vaults", nil)
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

	err = gc.Signature.Sign(request)
	if err != nil {
		return "", nil, err
	}

	response, err := gc.Client.Do(request)
	if err != nil {
		return "", nil, err
	}

	// TODO log x-amzn-RequestId

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
		responseVaults[i].CreationDate, err =
			time.Parse(time.RFC3339, vaults.VaultList[i].CreationDate)
		if err != nil {
			return "", nil, err
		}
		if vaults.VaultList[i].LastInventoryDate != nil {
			responseVaults[i].LastInventoryDate, err =
				time.Parse(time.RFC3339, *vaults.VaultList[i].LastInventoryDate)
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

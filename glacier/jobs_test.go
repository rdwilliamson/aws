package glacier

import (
	"encoding/json"
	"testing"
)

func TestGetInventoryJob(t *testing.T) {
	body := `{"VaultARN":"arn:aws:glacier:us-east-1:111111111111:vaults/dummy","InventoryDate":"2012-09-17T18:06:58Z","ArchiveList":[{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"A&M.gob","CreationDate":"2012-09-15T03:17:55Z","Size":14540,"SHA256TreeHash":"1d1b830976ffef07d42c0250d6f2a358f240f69cd4e8e69e261ea857974eb578"},{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"..\\tmp","CreationDate":"2012-09-15T18:36:59Z","Size":1048576,"SHA256TreeHash":"9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"},{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"A&M.7z","CreationDate":"2012-09-15T18:49:01Z","Size":145768841,"SHA256TreeHash":"8e1ae74c2f93fc6d474622047c16a94757d83f196364c268c1cbeef32fb180a6"},{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"Angra.7z","CreationDate":"2012-09-15T19:35:11Z","Size":26276377,"SHA256TreeHash":"603d4b7b632673c70cfee591dc31e1e51799b53ea3c28d7e1faf1aca7b6d76e5"},{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"Bahamas09.7z","CreationDate":"2012-09-16T18:13:45Z","Size":127647225,"SHA256TreeHash":"a34dc9310547925edf2591018db338dd981370d7da521fe8a24d445b0813bd2f"},{"ArchiveId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ArchiveDescription":"1048576","CreationDate":"2012-09-17T05:40:50Z","Size":45983625,"SHA256TreeHash":"1a1c8b77cab4855cb0883cfc695da772662e71dee97c7a828d0bf2e81f1aa89e"}]}`

	var i Inventory
	err := json.Unmarshal([]byte(body), &i)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListJobs(t *testing.T) {
	body := `{"JobList":[{"Action":"InventoryRetrieval","ArchiveId":null,"ArchiveSizeInBytes":null,"Completed":true,"CompletionDate":"2012-09-22T21:48:00.195Z","CreationDate":"2012-09-22T17:28:35.569Z","InventorySizeInBytes":2072,"JobDescription":"testinventory","JobId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","SHA256TreeHash":null,"SNSTopic":"arn:aws:sns:us-east-1:111111111111:glacier","StatusCode":"Succeeded","StatusMessage":"Succeeded","VaultARN":"arn:aws:glacier:us-east-1:111111111111:vaults/dummy"}],"Marker":null}`

	var j jobList
	err := json.Unmarshal([]byte(body), &j)
	if err != nil {
		t.Fatal(err)
	}
}

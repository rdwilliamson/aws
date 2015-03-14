package glacier

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/rdwilliamson/aws"
)

// DataRetrievalPolicy, you can choose from three types of Amazon Glacier data
// retrieval policies: Free Tier Only, Max Retrieval Rate, and No Retrieval
// Limit. By using a Free Tier Only policy, you can keep your retrievals within
// your daily free tier allowance and not incur any data retrieval cost. If you
// want to retrieve more data than the free tier, you can use a Max Retrieval
// Rate policy to set a bytes-per-hour retrieval rate limit. The Max Retrieval
// Rate policy ensures that the peak retrieval rate from all retrieval jobs
// across your account in a region does not exceed the bytes-per-hour limit you
// set. If you don't want to set a retrieval limit, you can use a No Retrieval
// Limit policy where all valid data retrieval requests will be accepted.
//
// With both Free Tier Only and Max Retrieval Rate policies, data retrieval
// requests that would exceed the retrieval limits you specified will not be
// accepted. If you use a Free Tier Only policy, Amazon Glacier will
// synchronously reject retrieval requests that would exceed your free tier
// allowance. If you use a Max Retrieval Rate policy, Amazon Glacier will reject
// retrieval requests that would cause the peak retrieval rate of the in
// progress jobs to exceed the bytes-per-hour limit set by the policy. These
// policies help you simplify data retrieval cost management.
//
// The following are some useful facts about data retrieval policies:
// - Data retrieval policy settings do not change the 3 to 5 hour period that it
//   takes to retrieve data from Amazon Glacier.
// - Setting a new data retrieval policy does not affect previously accepted
//   retrieval jobs that are already in progress.
// - If a retrieval job request is rejected because of a data retrieval policy,
//   you will not be charged for the job or the request.
//
// You can set one data retrieval policy for each AWS region, which will govern
// all data retrieval activities in the region under your account. A data
// retrieval policy is region-specific because data retrieval costs vary across
// AWS regions, and the 5 percent retrieval free tier is also computed based on
// your storage in each region. For more information, see
// http://aws.amazon.com/glacier/pricing/.
type DataRetrievalPolicy int

const (
	// InvalidDataRetrievalPolicy, the zero value for a DataRetrievalPolicy.
	InvalidDataRetrievalPolicy DataRetrievalPolicy = iota

	// BytesPerHour, you can set your data retrieval policy to Max Retrieval
	// Rate to control the peak retrieval rate by specifying a data retrieval
	// limit that has a bytes-per-hour maximum. When you set the data retrieval
	// policy to Max Retrieval Rate, a new retrieval request will be rejected if
	// it would cause the peak retrieval rate of the in progress jobs to exceed
	// the bytes-per-hour limit specified by the policy. If a retrieval job
	// request is rejected, you will receive an error message stating that the
	// request has been denied by the current data retrieval policy.
	//
	// Setting your data retrieval policy to the Max Retrieval Rate policy can
	// affect how much free tier you can use in a day. For example, suppose you
	// have 30 GB of free retrieval allowance a day and you decide to set Max
	// Retrieval Rate to 1 GB per hour. In this case, you can only retrieve 24
	// GB of data per day, even though that is less than your daily free tier.
	// To ensure you make good use of the daily free tier allowance, you can
	// first set your policy to Free Tier Only and then switch to the Max
	// Retrieval Rate policy later if you need to. For more information on how
	// your retrieval allowance is calculated, go to
	// http://aws.amazon.com/glacier/faqs/.
	BytesPerHour

	// FreeTier, you can set a data retrieval policy to Free Tier Only to ensure
	// that your retrievals will always stay within your free tier allowance, so
	// you don't incur data retrieval charges. If a retrieval request is
	// rejected, you will receive an error message stating that the request has
	// been denied by the current data retrieval policy.
	//
	// You set the data retrieval policy to Free Tier Only for a particular AWS
	// region. Once the policy is set, you cannot retrieve more data in a day
	// than your prorated daily free retrieval allowance for that region and you
	// will not incur data retrieval fees.
	//
	// You can switch to a Free Tier Only policy after you have incurred data
	// retrieval charges within a month. The Free Tier Only policy will take
	// effect for new retrieval requests, but will not affect past requests. You
	// will be billed for the previously incurred charges.
	FreeTier

	// None, if your data retrieval policy is set to No Retrieval Limit, all
	// valid data retrieval requests will be accepted and your data retrieval
	// costs will vary based on your usage.
	None
)

const _DataRetrievalPolicy_name = "BytesPerHourFreeTierNone"

var _DataRetrievalPolicy_index = [...]uint8{0, 12, 20, 24}

func (i DataRetrievalPolicy) String() string {
	i -= 1
	if i < 0 || i+1 >= DataRetrievalPolicy(len(_DataRetrievalPolicy_index)) {
		return "InvalidDataRetrievalPolicy"
	}
	return _DataRetrievalPolicy_name[_DataRetrievalPolicy_index[i]:_DataRetrievalPolicy_index[i+1]]
}

var toDataRetrievalPolicy = map[string]DataRetrievalPolicy{
	BytesPerHour.String():                  BytesPerHour,
	FreeTier.String():                      FreeTier,
	None.String():                          None,
	strings.ToLower(BytesPerHour.String()): BytesPerHour,
	strings.ToLower(FreeTier.String()):     FreeTier,
	strings.ToLower(None.String()):         None,
}

// ToDataRetrievalPolicy tries to converts a string to a DataRetrievalPolicy.
func ToDataRetrievalPolicy(strategy string) DataRetrievalPolicy {
	if result, ok := toDataRetrievalPolicy[strategy]; ok {
		return result
	}
	return toDataRetrievalPolicy[strings.ToLower(strings.Join(strings.Fields(strategy), ""))]
}

type dataRetrievalPolicy struct {
	Policy struct {
		Rules [1]struct {
			BytesPerHour *int
			Strategy     string
		}
	}
}

// This operation returns the current data retrieval policy and bytes per hour
// (if applicable).
//
// There is one policy per region for an AWS account.
//
// For more information about data retrieval policies, see DataRetrievalPolicy.
func (c *Connection) GetDataRetrievalPolicy() (DataRetrievalPolicy, int, error) {
	// Build request.
	request, err := http.NewRequest("GET", c.policy("data-retrieval"), nil)
	if err != nil {
		return 0, 0, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return 0, 0, aws.ParseError(response)
	}

	// Parse success response.
	var policy dataRetrievalPolicy
	decoder := json.NewDecoder(response.Body)
	err = decoder.Decode(&policy)
	if err != nil {
		return 0, 0, err
	}

	var bytesPerHour int
	if policy.Policy.Rules[0].BytesPerHour != nil {
		bytesPerHour = *policy.Policy.Rules[0].BytesPerHour
	}
	strategy := ToDataRetrievalPolicy(policy.Policy.Rules[0].Strategy)

	return strategy, bytesPerHour, nil
}

// This operation sets and then enacts a data retrieval policy.
//
// You can set one policy per region for an AWS account. The policy is enacted
// within a few minutes of a successful call.
//
// The set policy operation does not affect retrieval jobs that were in progress
// before the policy was enacted. For more information about data retrieval
// policies, see DataRetrievalPolicy.
func (c *Connection) SetRetrievalPolicy(drp DataRetrievalPolicy, bytesPerHour int) error {
	// Build request.
	var rules dataRetrievalPolicy
	rules.Policy.Rules[0].Strategy = drp.String()
	if bytesPerHour != 0 {
		rules.Policy.Rules[0].BytesPerHour = &bytesPerHour
	}
	data, err := json.Marshal(rules)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(data)

	request, err := http.NewRequest("PUT", c.policy("data-retrieval"), reader)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, aws.ReadSeekerPayload(reader))

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return aws.ParseError(response)
	}

	io.Copy(ioutil.Discard, response.Body)

	// Parse success response.
	return nil
}

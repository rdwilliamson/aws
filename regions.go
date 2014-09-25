package aws

// Collection of host address/endpoint for services offered in the region.
type Region struct {
	Region  string // Human readable name.
	Name    string // Canonical name.
	Glacier string // Glacier host address/endpoint.
}

// http://docs.amazonwebservices.com/general/latest/gr/rande.html
var (
	USEast1 = &Region{
		"US East (Northern Virginia)",
		"us-east-1",
		"glacier.us-east-1.amazonaws.com"}
	USWest1 = &Region{
		"US West (Northern California)",
		"us-west-1",
		"glacier.us-west-1.amazonaws.com"}
	USWest2 = &Region{
		"US West (Oregon)",
		"us-west-2",
		"glacier.us-west-2.amazonaws.com"}
	EU1 = &Region{
		"EU (Ireland)",
		"eu-west-1",
		"glacier.eu-west-1.amazonaws.com"}
	AsiaPacific1 = &Region{
		"Asia Pacific (Tokyo)",
		"ap-northeast-1",
		"glacier.ap-northeast-1.amazonaws.com"}
	AsiaPacific2 = &Region{
		"Asia Pacific (Sydney)",
		"ap-southeast-2",
		"glacier.ap-southeast-2.amazonaws.com",
	}
)

var Regions = []*Region{USEast1, USWest1, USWest2, EU1, AsiaPacific1, AsiaPacific2}

package aws

type Region struct {
	Region string // human readable name
	Name   string // canonical name
	// TODO CloudFormation Endpoint, CloundFront Endpoint etc.
	Glacier string
}

// http://docs.amazonwebservices.com/general/latest/gr/rande.html
var (
	USEast = &Region{
		"US East (Northern Virginia)",
		"us-east-1",
		"glacier.us-east-1.amazonaws.com"}
)

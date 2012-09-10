package aws

// Collection of host address/endpoint for services offered in the region.
type Region struct {
	Region  string // human readable name
	Name    string // canonical name
	Glacier string // host address/endpoint
}

// http://docs.amazonwebservices.com/general/latest/gr/rande.html
var (
	USEast = &Region{
		"US East (Northern Virginia)",
		"us-east-1",
		"glacier.us-east-1.amazonaws.com"}
)

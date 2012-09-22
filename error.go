package aws

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (e Error) Error() string {
	return e.Code + ": " + e.Type + ": " + e.Message
}

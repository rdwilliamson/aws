package aws

type Error struct {
	Message string `json:message`
	Code    string `json:code`
	Type    string `json:type`
}

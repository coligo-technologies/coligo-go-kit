package nats

type Response struct {
	StatusCode int    `json:"statusCode"`
	Message    string `json:"message"`
	Data       any    `json:"data"`
}

func NewResponse(code int, message string, data any) *Response {
	return &Response{
		StatusCode: code,
		Message:    message,
		Data:       data,
	}
}

func BadRequest(message string) *Response {
	return NewResponse(400, message, nil)
}

func BadGateway(message string) *Response {
	return NewResponse(502, message, nil)
}

func GatewayTimeout(message string) *Response {
	return NewResponse(504, message, nil)
}

func InternalServerError(message string) *Response {
	return NewResponse(500, message, nil)
}

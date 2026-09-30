package refund

type Account struct {
	BankID      string `json:"bankId"`
	AccountNo   string `json:"accountNo"`
	Name        string `json:"name"`
	AccountType string `json:"accountType"`
	IDNo        string `json:"idNo"`
	IDType      string `json:"idType"`
}

type Invoice struct {
	OrderID             string `json:"orderId"`
	RefundAmount        *int64 `json:"refundAmount"`
	Reason              string `json:"reason"`
	Passengers          string `json:"passengers"`
	OriginalOrderNumber string `json:"originalOrderNumber"`
	NotifyURL           string `json:"notifyUrl"`
	TicketOffice        string `json:"ticketOffice"`
}

type CreateRequest struct {
	ReqData struct {
		Account Account `json:"account"`
		Invoice Invoice `json:"invoice"`
	} `json:"reqData"`
	SignMsg    string `json:"signMsg"`
	TicketCall *int64 `json:"ticketCall,omitempty"`
}

type StatusRequest struct {
	ReqData struct {
		Invoice struct {
			OrderID string `json:"orderId"`
		} `json:"invoice"`
	} `json:"reqData"`
	SignMsg string `json:"signMsg"`
}

type Response struct {
	RetCode int          `json:"retCode"`
	RetMsg  string       `json:"retMsg"`
	RetData ResponseData `json:"retData"`
	SignMsg string       `json:"signMsg"`
}

type ResponseData struct {
	Invoice any `json:"invoice"`
}

type CreateInvoice struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

type StatusInvoice struct {
	Balance      any     `json:"balance"`
	BankCode     string  `json:"bankCode"`
	BankNo       *string `json:"bankNo,omitempty"`
	Comment      *string `json:"comment,omitempty"`
	CurType      string  `json:"curType"`
	Fee          string  `json:"fee"`
	MWNo         string  `json:"mwNo"`
	OrderID      string  `json:"orderId"`
	PGCode       string  `json:"pgCode"`
	Rate         string  `json:"rate"`
	RefundAmount any     `json:"refundAmount"`
	Status       string  `json:"status"`
	TradeTime    *string `json:"tradeTime,omitempty"`
}

type PublicError struct {
	HTTPStatus int
	Payload    any
	Cause      error
}

func (e *PublicError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "refund request failed"
}

func ValidateCreate(request CreateRequest) []string {
	var problems []string
	account := request.ReqData.Account
	invoice := request.ReqData.Invoice
	for _, field := range []struct{ name, value string }{
		{"reqData.account.bankId", account.BankID}, {"reqData.account.accountNo", account.AccountNo},
		{"reqData.account.name", account.Name}, {"reqData.account.accountType", account.AccountType},
		{"reqData.account.idNo", account.IDNo}, {"reqData.invoice.orderId", invoice.OrderID},
		{"reqData.invoice.reason", invoice.Reason}, {"reqData.invoice.passengers", invoice.Passengers},
		{"reqData.invoice.originalOrderNumber", invoice.OriginalOrderNumber}, {"reqData.invoice.notifyUrl", invoice.NotifyURL},
		{"reqData.invoice.ticketOffice", invoice.TicketOffice}, {"signMsg", request.SignMsg},
	} {
		if field.value == "" {
			problems = append(problems, field.name+" should not be empty")
		}
	}
	if account.IDType != "1" && account.IDType != "2" && account.IDType != "3" {
		problems = append(problems, "reqData.account.idType must be one of the following values: 1, 2, 3")
	}
	if invoice.RefundAmount == nil {
		problems = append(problems, "reqData.invoice.refundAmount must be a number conforming to the specified constraints")
	}
	return problems
}

func ValidateStatus(request StatusRequest) []string {
	var problems []string
	if request.ReqData.Invoice.OrderID == "" {
		problems = append(problems, "reqData.invoice.orderId should not be empty")
	}
	if request.SignMsg == "" {
		problems = append(problems, "signMsg should not be empty")
	}
	return problems
}

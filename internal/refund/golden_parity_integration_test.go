//go:build integration

package refund

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"bridgepay-refund-go/internal/signing"
	"bridgepay-refund-go/internal/storage"
)

func TestNodeAndGoMatchGoldenRefundFixture(t *testing.T) {
	nodeRoot := os.Getenv("TEST_NODE_REFUND_ROOT")
	if nodeRoot == "" {
		t.Skip("TEST_NODE_REFUND_ROOT is not set")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	fixturePath := filepath.Join(root, "docs", "contracts", "fixtures", "golden-contracts.json")
	var fixture struct {
		Cases []struct {
			Name     string        `json:"name"`
			Request  CreateRequest `json:"request"`
			Expected struct {
				RetData struct {
					Invoice CreateInvoice `json:"invoice"`
				} `json:"retData"`
			} `json:"expected"`
		} `json:"cases"`
	}
	raw, err := os.ReadFile(fixturePath)
	if err != nil || json.Unmarshal(raw, &fixture) != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var golden CreateRequest
	var expectedInvoice CreateInvoice
	for _, item := range fixture.Cases {
		if item.Name == "refund-create-success-pending" {
			golden, expectedInvoice = item.Request, item.Expected.RetData.Invoice
		}
	}
	if golden.ReqData.Invoice.RefundAmount == nil {
		t.Fatal("golden refund-create case is missing")
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "parity-private.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(t, key)})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "scripts", "node-golden-parity.cjs")
	command := exec.CommandContext(context.Background(), "node", script, fixturePath, keyPath)
	command.Env = append(os.Environ(), "NODE_REFUND_ROOT="+nodeRoot)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Node fixture runner: %v: %s", err, output)
	}
	var node struct {
		AmountData         amountData `json:"amountData"`
		MaskedAccount      string     `json:"maskedAccount"`
		Status             string     `json:"status"`
		ValidationRejected bool       `json:"validationRejected"`
		Signature          string     `json:"signature"`
		Payout             struct {
			RequestData   []PayoutRequest `json:"requestData"`
			PayloadRefund PayoutRequest   `json:"payloadRefund"`
		} `json:"payout"`
	}
	if err := json.Unmarshal(output, &node); err != nil {
		t.Fatalf("decode Node result: %v: %s", err, output)
	}

	amount := amountBreakdown(*golden.ReqData.Invoice.RefundAmount, 2100, 11)
	expectedPayout := PayoutRequest{
		ReferenceID: golden.ReqData.Invoice.OrderID, ChannelCode: "ID_" + golden.ReqData.Account.BankID,
		ChannelProperties: PayoutChannelProperties{AccountNumber: golden.ReqData.Account.AccountNo, AccountHolderName: golden.ReqData.Account.Name},
		Amount:            *golden.ReqData.Invoice.RefundAmount, Description: golden.ReqData.Invoice.Reason,
		Currency: "IDR", IdempotencyKey: golden.ReqData.Invoice.OrderID,
	}
	if node.AmountData != amount || node.MaskedAccount != mask(golden.ReqData.Account.AccountNo, 4) ||
		node.Status != statusWording(storage.RefundPendingDisbursement) || node.Payout.PayloadRefund != expectedPayout {
		t.Fatalf("Node/Go fixture mismatch: node=%+v amount=%+v payout=%+v", node, amount, expectedPayout)
	}
	if len(node.Payout.RequestData) != 1 || node.Payout.RequestData[0].ChannelProperties.AccountNumber != mask(golden.ReqData.Account.AccountNo, 4) {
		t.Fatalf("Node masked request history = %+v", node.Payout.RequestData)
	}
	var invalid CreateRequest
	if len(ValidateCreate(invalid)) == 0 || !node.ValidationRejected {
		t.Fatal("Node and Go must both reject the invalid golden request")
	}
	signer, err := signing.Load(keyPath, "")
	if err != nil {
		t.Fatal(err)
	}
	signedPayload, _ := json.Marshal(struct {
		Invoice CreateInvoice `json:"invoice"`
	}{Invoice: expectedInvoice})
	goSignature, err := signer.Sign(signedPayload)
	if err != nil {
		t.Fatal(err)
	}
	if node.Signature != goSignature {
		t.Fatalf("signature mismatch: node=%s go=%s", base64Prefix(node.Signature), base64Prefix(goSignature))
	}
}

func mustPKCS8(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func base64Prefix(value string) string {
	if _, err := base64.StdEncoding.DecodeString(value); err != nil {
		return "invalid"
	}
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

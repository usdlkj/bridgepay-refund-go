//go:build integration

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" && os.Getenv("TEST_DB_USERNAME") != "" {
		u := &url.URL{Scheme: "postgres", User: url.UserPassword(os.Getenv("TEST_DB_USERNAME"), os.Getenv("TEST_DB_PASSWORD")), Host: net.JoinHostPort(envTestDefault("TEST_DB_HOST", "127.0.0.1"), envTestDefault("TEST_DB_PORT", "5432")), Path: envTestDefault("TEST_DB_DATABASE", "kcic_refund")}
		query := u.Query()
		query.Set("sslmode", "disable")
		u.RawQuery = query.Encode()
		databaseURL = u.String()
	}
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func envTestDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func TestExistingNodeSchemaIsCompatible(t *testing.T) {
	pool := integrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := VerifySchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRefundIsAtomicDuplicateSafeAndLockable(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	gaNumber := "SYNTHETIC-COMPAT-" + NewID()
	detailID := NewID()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_detail_tickets WHERE refund_detail_id = $1`, detailID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refunds WHERE refund_ga_number = $1`, gaNumber)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_details WHERE id = $1`, detailID)
	})

	input := RefundInsert{
		RefundGANumber: gaNumber,
		Status:         RefundPendingChecking,
		Amount:         "1000",
		AmountData:     json.RawMessage(`{"amount":1000,"fee":2100,"tax":231,"totalAmount":3331}`),
		Data:           json.RawMessage(`{"reqData":{"account":{"accountNo":"******0001","idNo":"********1234"},"invoice":{"orderId":"` + gaNumber + `"}}}`),
		BankData:       json.RawMessage(`{"bankCode":"ID_BCA","bankNumber":"******0001"}`),
		RequestData:    json.RawMessage(`{"reference_id":"` + gaNumber + `","account_number":"******0001"}`),
		Detail: &RefundDetailInsert{
			ID: detailID, RefundGANumber: gaNumber, Amount: "1000", Reason: "synthetic compatibility test",
		},
	}
	created, err := store.CreateRefund(ctx, input)
	if err != nil {
		t.Fatalf("CreateRefund() error = %v", err)
	}
	if created.RefundGANumber == nil || *created.RefundGANumber != gaNumber {
		t.Fatalf("unexpected created refund: %+v", created)
	}

	input.ID = NewID()
	input.Detail.ID = NewID()
	if _, err := store.CreateRefund(ctx, input); !errors.Is(err, ErrDuplicateRefund) {
		t.Fatalf("duplicate error = %v", err)
	}
	var duplicateDetailCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM refund_details WHERE id = $1`, input.Detail.ID).Scan(&duplicateDetailCount); err != nil {
		t.Fatal(err)
	}
	if duplicateDetailCount != 0 {
		t.Fatal("duplicate transaction left a partial refund_details row")
	}

	err = store.WithTx(ctx, func(tx pgx.Tx) error {
		locked, err := store.LockRefundByGANumber(ctx, tx, gaNumber)
		if err != nil {
			return err
		}
		if locked.ID != created.ID {
			t.Fatalf("locked refund ID = %s, want %s", locked.ID, created.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentRefundCreationHasOneWinner(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	gaNumber := "SYNTHETIC-RACE-" + NewID()
	detailIDs := []string{NewID(), NewID()}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refunds WHERE refund_ga_number = $1`, gaNumber)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_details WHERE id = ANY($1)`, detailIDs)
	})

	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, detailID := range detailIDs {
		detailID := detailID
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.CreateRefund(ctx, RefundInsert{
				RefundGANumber: gaNumber,
				Status:         RefundPendingChecking,
				Amount:         "1000",
				Data:           json.RawMessage(`{"reqData":{"account":{"accountNo":"******0001","idNo":"********1234"}}}`),
				Detail:         &RefundDetailInsert{ID: detailID, RefundGANumber: gaNumber, Amount: "1000"},
			})
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes, duplicates := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrDuplicateRefund):
			duplicates++
		default:
			t.Fatalf("unexpected race result: %v", err)
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("successes=%d duplicates=%d", successes, duplicates)
	}
	var refundCount, detailCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM refunds WHERE refund_ga_number = $1`, gaNumber).Scan(&refundCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM refund_details WHERE id = ANY($1)`, detailIDs).Scan(&detailCount); err != nil {
		t.Fatal(err)
	}
	if refundCount != 1 || detailCount != 1 {
		t.Fatalf("refund rows=%d detail rows=%d", refundCount, detailCount)
	}
}

func TestConcurrentBankDataCreationReusesEncryptedWinner(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hash := "SYNTHETIC-HASH-" + NewID()
	ciphertext := json.RawMessage(`{"enc":"AA==","iv":"AA==","tag":"AA==","edk":"AA==","alg":"AES-256-GCM","kmd":{}}`)
	var createdIDs []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM bank_datas WHERE bank_code = 'SYNTHETIC_COMPAT' AND account_number_hash = $1`, hash)
	})

	type outcome struct {
		record  BankData
		created bool
		err     error
	}
	results := make(chan outcome, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			record, created, err := store.GetOrCreateBankData(ctx, BankDataInsert{
				BankCode: "SYNTHETIC_COMPAT", AccountNumberHash: hash, AccountNumberEnc: ciphertext,
			})
			results <- outcome{record: record, created: created, err: err}
		}()
	}
	wait.Wait()
	close(results)
	createdCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.created {
			createdCount++
		}
		createdIDs = append(createdIDs, result.record.ID)
	}
	if createdCount != 1 || len(createdIDs) != 2 || createdIDs[0] != createdIDs[1] {
		t.Fatalf("created=%d ids=%v", createdCount, createdIDs)
	}
}

func TestBankCatalogueSyncAndLegalIlumaTransitions(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code := "SYNTHETIC_" + NewID()
	hash := "SYNTHETIC_HASH_" + NewID()
	var bankID, bankDataID string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if bankDataID != "" {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM bank_datas WHERE id=$1`, bankDataID)
		}
		if bankID != "" {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_banks WHERE id=$1`, bankID)
		}
	})

	bank, err := store.UpsertXenditRefundBank(ctx, "Synthetic Bank", code, json.RawMessage(`{"channelCode":"`+code+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	bankID = bank.ID
	if bank.Status != BankDisabled {
		t.Fatalf("new bank status = %s", bank.Status)
	}
	enabled := BankEnabled
	bank, err = store.UpdateRefundBank(ctx, bank.ID, &enabled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bank.Status != BankEnabled {
		t.Fatalf("updated bank status = %s", bank.Status)
	}
	bank, err = store.UpsertXenditRefundBank(ctx, "Synthetic Bank Renamed", code, json.RawMessage(`{"channelCode":"`+code+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	if bank.ID != bankID || bank.Status != BankDisabled {
		t.Fatalf("resynced bank = %#v", bank)
	}
	updated, err := store.UpdateIlumaRefundBankByName(ctx, "Synthetic Bank Renamed", "999", json.RawMessage(`{"name":"Synthetic Bank Renamed","code":"999"}`))
	if err != nil || !updated {
		t.Fatalf("Iluma update = %v, %v", updated, err)
	}

	ciphertext := json.RawMessage(`{"enc":"AA==","iv":"AA==","tag":"AA==","edk":"AA==","alg":"AES-256-GCM","kmd":{}}`)
	data, _, err := store.GetOrCreateBankData(ctx, BankDataInsert{BankCode: code, AccountNumberHash: hash, AccountNumberEnc: ciphertext, Status: AccountPending, Result: AccountResultPending, LastCheckAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	bankDataID = data.ID
	if ok, err := store.ExpireBankData(ctx, data.ID); err != nil || !ok {
		t.Fatalf("expire = %v, %v", ok, err)
	}
	if ok, err := store.CompleteBankData(ctx, data.ID, AccountResultSuccess, json.RawMessage(`{"status":"completed"}`), nil, nil); err != nil || ok {
		t.Fatalf("expired row was revived: %v, %v", ok, err)
	}
	data, err = store.GetBankDataByID(ctx, data.ID)
	if err != nil {
		t.Fatal(err)
	}
	if data.AccountStatus != AccountExpired || data.AccountResult == nil || *data.AccountResult != AccountResultFailed {
		t.Fatalf("expired data = %#v", data)
	}
}

func TestRefundIntentAndPayoutResultAreDurableInOrder(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	orderID := "SYNTHETIC-PAYOUT-" + NewID()
	hash := "SYNTHETIC-PAYOUT-HASH-" + NewID()
	detailID := NewID()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_detail_tickets WHERE refund_detail_id=$1`, detailID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refunds WHERE refund_ga_number=$1`, orderID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_details WHERE id=$1`, detailID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM bank_datas WHERE bank_code='ID_SYNTHETIC' AND account_number_hash=$1`, hash)
	})
	ciphertext := json.RawMessage(`{"enc":"AA==","iv":"AA==","tag":"AA==","edk":"AA==","alg":"AES-256-GCM","kmd":{}}`)
	created, err := store.CreateRefund(ctx, RefundInsert{
		RefundGANumber: orderID, Status: RefundPendingChecking, Amount: "10000",
		AmountData: json.RawMessage(`{"amount":10000,"fee":2100,"AmountAfterFee":12100,"tax":231,"totalAmount":12331}`),
		Data:       json.RawMessage(`{"reqData":{"account":{"accountNo":"******7890","idNo":"*****4321"},"invoice":{"orderId":"` + orderID + `"}}}`),
		BankData:   json.RawMessage(`{"bankCode":"ID_SYNTHETIC","bankNumber":"******7890"}`), RequestData: json.RawMessage(`[]`),
		BankDataCreate: &BankDataInsert{BankCode: "ID_SYNTHETIC", AccountNumberEnc: ciphertext, AccountNumberHash: hash, Status: AccountPending, Result: AccountResultPending, LastCheckAt: time.Now()},
		Detail:         &RefundDetailInsert{ID: detailID, RefundID: NewID(), RefundGANumber: orderID, Amount: "0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.BankDataID == nil {
		t.Fatal("bank data was not linked atomically")
	}
	intent := json.RawMessage(`{"reference_id":"` + orderID + `","channel_properties":{"account_number":"******7890"},"idempotencyKey":"` + orderID + `"}`)
	reserved, err := store.AppendRefundRequestIntent(ctx, created.ID, intent)
	if err != nil {
		t.Fatal(err)
	}
	if string(reserved.RequestData) == "" || string(reserved.RequestData) == "[]" {
		t.Fatalf("request data=%s", reserved.RequestData)
	}
	payoutID := "SYNTHETIC-PAYOUT-ID"
	completed, err := store.RecordRefundPayoutSuccess(ctx, created.ID, &payoutID, json.RawMessage(`{"id":"SYNTHETIC-PAYOUT-ID","channel_properties":{"account_number":"******7890"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status == nil || *completed.Status != RefundPendingDisbursement || completed.DisbursementID == nil || *completed.DisbursementID != payoutID {
		t.Fatalf("completed=%#v", completed)
	}
	if err := store.RecordRefundPayoutFailure(ctx, created.ID, "synthetic signer failure", nil); err != nil {
		t.Fatal(err)
	}
	failed, err := store.GetRefundByGANumber(ctx, orderID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status == nil || *failed.Status != RefundFail || failed.RejectReason == nil || *failed.RejectReason != "synthetic signer failure" {
		t.Fatalf("failed=%#v", failed)
	}
}

func TestStep98BackofficeRetryAndReportPersistence(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	orderID := "SYNTHETIC-ADMIN-" + NewID()
	reportID := ""
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if reportID != "" {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM reports WHERE id=$1`, reportID)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refunds WHERE refund_ga_number=$1`, orderID)
	})
	created, err := store.CreateRefund(ctx, RefundInsert{
		RefundGANumber: orderID, Status: RefundFail, Amount: "10000",
		AmountData: json.RawMessage(`{"amount":10000}`), Data: json.RawMessage(`{"reqData":{"account":{"name":"Synthetic"}}}`),
		BankData: json.RawMessage(`{"bankCode":"ID_BCA","bankNumber":"******7890"}`), RequestData: json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := json.RawMessage(`{"reference_id":"` + orderID + `-1","channel_properties":{"account_number":"******7890"}}`)
	reserved, err := store.ReserveRefundRetry(ctx, created.ID, 0, 1, time.Now(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Status == nil || *reserved.Status != RefundRetry || !json.Valid(reserved.RetryAttempt) {
		t.Fatalf("reserved=%#v", reserved)
	}
	if _, err := store.ReserveRefundRetry(ctx, created.ID, 1, 1, time.Now(), intent); !errors.Is(err, ErrRetryLimit) && !errors.Is(err, ErrRetryNotAllowed) {
		t.Fatalf("second retry error=%v", err)
	}
	listed, err := store.ListRefunds(ctx, []RefundFilter{{Column: 0, Value: orderID}})
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed=%d error=%v", len(listed), err)
	}

	zone, _ := time.LoadLocation("Asia/Jakarta")
	day := time.Date(2026, 9, 20, 0, 0, 0, 0, zone)
	report, err := store.CreateReport(ctx, ReportRefund, "refund20092026", day, day)
	if err != nil {
		t.Fatal(err)
	}
	reportID = report.ID
	if err := store.CompleteReport(ctx, report.ID, json.RawMessage(`[{"seqNo":1}]`)); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetReport(ctx, report.ID)
	if err != nil || completed.Status != ReportCompleted || string(completed.Data) != `[{"seqNo": 1}]` {
		t.Fatalf("completed=%#v error=%v", completed, err)
	}
	locked, err := store.WithAdvisoryLock(ctx, "synthetic-step-9.8", func(context.Context) error { return nil })
	if err != nil || !locked {
		t.Fatalf("locked=%v error=%v", locked, err)
	}
}

func TestRefundWebhookIsDurablyDeduplicatedAndTransitionsOnce(t *testing.T) {
	pool := integrationPool(t)
	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	orderID := "SYNTHETIC-WEBHOOK-" + NewID()
	detailID := NewID()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM "ticketing-call-logs" WHERE refund_number=$1`, orderID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_webhook_calls WHERE refund_ref=$1`, orderID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refunds WHERE refund_ga_number=$1`, orderID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refund_details WHERE id=$1`, detailID)
	})
	created, err := store.CreateRefund(ctx, RefundInsert{
		RefundGANumber: orderID, Status: RefundPendingDisbursement, Amount: "10000",
		AmountData: json.RawMessage(`{"amount":10000,"fee":2100,"AmountAfterFee":12100,"tax":231,"totalAmount":12331}`),
		Data:       json.RawMessage(`{"reqData":{"account":{"bankId":"BCA","accountNo":"******7890","name":"Synthetic","accountType":"saving","idNo":"*****4321","idType":"1"},"invoice":{"orderId":"` + orderID + `","refundAmount":10000,"reason":"synthetic","passengers":"P","originalOrderNumber":"O","notifyUrl":"https://ticketing.example/refund","ticketOffice":"kcic"}},"signMsg":"sig"}`),
		Detail:     &RefundDetailInsert{ID: detailID, RefundGANumber: orderID, Amount: "10000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"event":"payout.succeeded","business_id":"business","created":"2026-09-29T01:00:00Z","data":{"id":"payout-1","amount":10000,"channel_code":"ID_BCA","currency":"IDR","status":"SUCCEEDED","reference_id":"` + orderID + `","created":"2026-09-29T01:00:00Z","updated":"2026-09-29T02:00:00Z","channel_properties":{"account_number":"******7890"}}}`)
	type callbackResult struct {
		id, processID string
		err           error
	}
	results := make(chan callbackResult, 2)
	for range 2 {
		id := NewID()
		go func() {
			processID, err := store.InsertRefundWebhookCall(ctx, RefundWebhookInsert{ID: id, RefundReference: orderID, Source: "xendit", DedupeKey: "dedupe-" + orderID, Payload: payload})
			results <- callbackResult{id: id, processID: processID, err: err}
		}()
	}
	var processID string
	var received []callbackResult
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		received = append(received, result)
		if result.id == result.processID {
			processID = result.processID
		}
	}
	if processID == "" {
		t.Fatal("no callback delivery became the processing row")
	}
	for _, result := range received {
		if result.processID != processID {
			t.Fatalf("callback %s process ID=%s want=%s", result.id, result.processID, processID)
		}
	}
	var callbackCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM refund_webhook_calls WHERE refund_ref=$1`, orderID).Scan(&callbackCount); err != nil || callbackCount != 2 {
		t.Fatalf("callback rows=%d error=%v", callbackCount, err)
	}
	work, err := store.PrepareRefundWebhook(ctx, processID, "", "", "", nil, 1, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if work.State != WebhookFollowUp || work.Outcome != "success" || work.Refund == nil || work.Refund.ID != created.ID {
		t.Fatalf("work=%+v", work)
	}
	if err := store.CompleteRefundWebhook(ctx, processID, "success", json.RawMessage(`{"payload":{"retData":{"bankNo":"******7890"}},"responseData":{"retCode":0}}`), 200); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetRefundByGANumber(ctx, orderID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status == nil || *completed.Status != RefundDone || len(completed.NotificationLog) == 0 {
		t.Fatalf("completed refund=%+v", completed)
	}
}

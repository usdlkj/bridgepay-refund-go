package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

var requiredColumns = map[string][]string{
	"api_log_debugs":        splitColumns("id endpoint payload signature signature_status raw_payload created_at updated_at"),
	"bank_datas":            splitColumns("id bank_code account_number_enc account_number_hash request_id account_status account_result iluma_data failure_code failure_message last_check_at created_at updated_at deleted_at"),
	"configurations":        splitColumns("id config_name config_value created_at updated_at"),
	"iluma_call_logs":       splitColumns("id url method payload response func request created_at updated_at deleted_at"),
	"iluma_callbacks":       splitColumns("id callback_type request_number payload response response_at created_at updated_at deleted_at"),
	"payment_gateways":      splitColumns("id pg_code pg_name credential credential_encrypted status weight percentage_range credential_enc credential_iv credential_tag credential_edk credential_alg credential_kmd created_at updated_at deleted_at"),
	"refund_banks":          splitColumns("id bank_name xendit_code xendit_data iluma_code iluma_data bank_status created_at updated_at deleted_at"),
	"refund_detail_tickets": splitColumns("id arrival_station cars_number departure_date departure_station identity_number identity_type name order_number purchase_price seat_number ticket_class ticket_number refund_detail_id created_at updated_at deleted_at"),
	"refund_details":        splitColumns("id refund_id email phone_number reason refund_ga_number refund_amount ticket_office created_at updated_at deleted_at"),
	"refund_logs":           splitColumns("id type location detail msg notes created_at updated_at deleted_at"),
	"refund_webhook_calls":  splitColumns("id refund_ref source payload response response_status refund_id created_at updated_at"),
	"refunds":               splitColumns("id refund_ga_number refund_status refund_amount refund_amount_data refund_data refund_reason reject_reason reject_by approval_fin_by approval_rbd_by approval_fin_at approval_rbd_at reject_at refund_bank_data refund_date request_data retry_attempt retry_date target_refund_date refund_execute_data notif_log refund_detail_id disbursement_id disbursement_response bank_data_id created_at updated_at deleted_at"),
	"report_data_rows":      splitColumns("id seq_no refund_date cancel_time refund_type refund_person refund_charge refund_charge_tax refund_amount refund_trande_no plat_trade_no refund_bank_code refund_bank_name refund_account refund_account_name actual_refund_amount passenger_name encrypted_id_number nationality order_number ticket_no ticketing_station business_area office_no window_no shift_no operator_name ticketing_time departure_time train_no origin cars_number seat_number origin_code purchase_date destination destination_code arrival_time seat_class ticket_type original_ticket_price report_id"),
	"reports":               splitColumns("id name refund_start_date refund_end_date report_data report_type report_status created_at updated_at deleted_at"),
	"ticketing-call-logs":   splitColumns("id refund_number payload response created_at updated_at"),
}

var requiredTypes = map[string]string{
	"refunds.id":                            "varchar",
	"refunds.refund_ga_number":              "varchar",
	"refunds.refund_amount":                 "numeric",
	"refunds.refund_data":                   "jsonb",
	"bank_datas.id":                         "varchar",
	"bank_datas.account_number_enc":         "jsonb",
	"bank_datas.account_number_hash":        "varchar",
	"bank_datas.account_status":             "bank_datas_account_status_enum",
	"bank_datas.account_result":             "bank_datas_account_result_enum",
	"refund_webhook_calls.payload":          "jsonb",
	"refund_detail_tickets.identity_number": "varchar",
	"reports.report_data":                   "jsonb",
}

var requiredEnums = map[string][]string{
	"api_log_debugs_signature_status_enum": {"accepted", "rejected"},
	"bank_datas_account_result_enum":       {"success", "failed", "pending"},
	"bank_datas_account_status_enum":       {"pending", "completed", "expired"},
	"payment_gateways_status_enum":         {"production", "development", "disable"},
}

type schemaColumn struct {
	table, name, dataType, udtName string
}

func VerifySchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT table_name, column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public'`)
	if err != nil {
		return fmt.Errorf("query schema columns: %w", err)
	}
	defer rows.Close()
	actual := make(map[string]schemaColumn)
	for rows.Next() {
		var column schemaColumn
		if err := rows.Scan(&column.table, &column.name, &column.dataType, &column.udtName); err != nil {
			return fmt.Errorf("scan schema column: %w", err)
		}
		actual[column.table+"."+column.name] = column
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema columns: %w", err)
	}

	var problems []string
	for table, columns := range requiredColumns {
		for _, column := range columns {
			if _, ok := actual[table+"."+column]; !ok {
				problems = append(problems, "missing "+table+"."+column)
			}
		}
	}
	if _, plaintextColumn := actual["bank_datas.account_number"]; plaintextColumn {
		problems = append(problems, "unsafe legacy bank_datas.account_number column is present")
	}
	for key, expected := range requiredTypes {
		column, ok := actual[key]
		if !ok {
			continue
		}
		if column.udtName != expected {
			problems = append(problems, fmt.Sprintf("%s type is %s, expected %s", key, column.udtName, expected))
		}
	}

	for enumName, expected := range requiredEnums {
		labels, err := enumLabels(ctx, pool, enumName)
		if err != nil {
			return err
		}
		if strings.Join(labels, ",") != strings.Join(expected, ",") {
			problems = append(problems, fmt.Sprintf("enum %s is [%s], expected [%s]", enumName, strings.Join(labels, ","), strings.Join(expected, ",")))
		}
	}

	for _, required := range []struct{ table, columns string }{
		{"refunds", "refund_ga_number"},
		{"bank_datas", "bank_code,account_number_hash"},
	} {
		ok, err := hasUniqueIndex(ctx, pool, required.table, strings.Split(required.columns, ","))
		if err != nil {
			return err
		}
		if !ok {
			problems = append(problems, "missing unique index on "+required.table+"("+required.columns+")")
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("incompatible refund schema: %s", strings.Join(problems, "; "))
	}
	return nil
}

func enumLabels(ctx context.Context, pool *pgxpool.Pool, name string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON e.enumtypid = t.oid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public' AND t.typname = $1
		ORDER BY e.enumsortorder`, name)
	if err != nil {
		return nil, fmt.Errorf("query enum %s: %w", name, err)
	}
	defer rows.Close()
	var labels []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	return labels, rows.Err()
}

func hasUniqueIndex(ctx context.Context, pool *pgxpool.Pool, table string, columns []string) (bool, error) {
	var found bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_index i
			JOIN pg_class t ON t.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = 'public' AND t.relname = $1 AND i.indisunique
			AND (
				SELECT array_agg(a.attname::text ORDER BY k.ordinality)
				FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality)
				JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
			) = $2::text[]
		)`, table, columns).Scan(&found)
	return found, err
}

func splitColumns(value string) []string { return strings.Fields(value) }

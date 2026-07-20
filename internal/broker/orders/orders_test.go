package orders

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type recordingOrdersClient struct {
	investapi.OrdersServiceClient
	postRequest      *investapi.PostOrderRequest
	asyncRequest     *investapi.PostOrderAsyncRequest
	replaceRequest   *investapi.ReplaceOrderRequest
	getOrdersRequest *investapi.GetOrdersRequest
}

func (c *recordingOrdersClient) GetOrders(
	_ context.Context,
	req *investapi.GetOrdersRequest,
	_ ...grpc.CallOption,
) (*investapi.GetOrdersResponse, error) {
	c.getOrdersRequest = req
	return &investapi.GetOrdersResponse{}, nil
}

func (c *recordingOrdersClient) PostOrder(
	_ context.Context,
	req *investapi.PostOrderRequest,
	_ ...grpc.CallOption,
) (*investapi.PostOrderResponse, error) {
	c.postRequest = req
	return &investapi.PostOrderResponse{}, nil
}

func (c *recordingOrdersClient) PostOrderAsync(
	_ context.Context,
	req *investapi.PostOrderAsyncRequest,
	_ ...grpc.CallOption,
) (*investapi.PostOrderAsyncResponse, error) {
	c.asyncRequest = req
	return &investapi.PostOrderAsyncResponse{}, nil
}

func (c *recordingOrdersClient) ReplaceOrder(
	_ context.Context,
	req *investapi.ReplaceOrderRequest,
	_ ...grpc.CallOption,
) (*investapi.PostOrderResponse, error) {
	c.replaceRequest = req
	return &investapi.PostOrderResponse{}, nil
}

func TestConfirmMarginTradeIsForwarded(t *testing.T) {
	recorder := &recordingOrdersClient{}
	client := Client{api: recorder}
	params := PlaceParams{
		AccountID: "acc", InstrumentID: "uid", OrderID: "00000000-0000-4000-8000-000000000001",
		Direction: investapi.OrderDirection_ORDER_DIRECTION_BUY,
		OrderType: investapi.OrderType_ORDER_TYPE_LIMIT,
		Lots:      1, Price: &investapi.Quotation{Units: 100}, ConfirmMarginTrade: true,
	}

	if _, err := client.Place(context.Background(), params); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if !recorder.postRequest.GetConfirmMarginTrade() {
		t.Error("PostOrderRequest.confirm_margin_trade = false, want true")
	}

	if _, err := client.PlaceAsync(context.Background(), params); err != nil {
		t.Fatalf("PlaceAsync: %v", err)
	}
	if !recorder.asyncRequest.GetConfirmMarginTrade() {
		t.Error("PostOrderAsyncRequest.confirm_margin_trade = false, want true")
	}

	if _, err := client.Replace(context.Background(), ReplaceParams{
		AccountID: "acc", OrderID: "exchange-id",
		IdempotencyKey: "00000000-0000-4000-8000-000000000002",
		Lots:           1, Price: &investapi.Quotation{Units: 101}, ConfirmMarginTrade: true,
	}); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if !recorder.replaceRequest.GetConfirmMarginTrade() {
		t.Error("ReplaceOrderRequest.confirm_margin_trade = false, want true")
	}
}

// TestListVsListToday proves the two order-list lookups differ exactly where
// reconciliation depends on it: List uses the active-only default (no filter),
// while ListToday sets execution_status to the full status set so terminal
// orders created today are included (findings F3/F4).
func TestListVsListToday(t *testing.T) {
	recorder := &recordingOrdersClient{}
	client := Client{api: recorder}

	if _, err := client.List(context.Background(), "acc"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if recorder.getOrdersRequest.GetAdvancedFilters() != nil {
		t.Errorf("List must not set advanced_filters (active-only default), got %+v", recorder.getOrdersRequest.GetAdvancedFilters())
	}

	if _, err := client.ListToday(context.Background(), "acc"); err != nil {
		t.Fatalf("ListToday: %v", err)
	}
	statuses := recorder.getOrdersRequest.GetAdvancedFilters().GetExecutionStatus()
	want := map[investapi.OrderExecutionReportStatus]bool{
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL:          true,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_PARTIALLYFILL: true,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED:     true,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED:      true,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW:           true,
	}
	if len(statuses) != len(want) {
		t.Fatalf("ListToday execution_status = %v, want the full non-unspecified set", statuses)
	}
	for _, s := range statuses {
		if !want[s] {
			t.Errorf("unexpected status %v in ListToday filter", s)
		}
		delete(want, s)
	}
	if len(want) != 0 {
		t.Errorf("ListToday filter missing statuses: %v", want)
	}
}

package orders

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

type recordingOrdersClient struct {
	investapi.OrdersServiceClient
	postRequest    *investapi.PostOrderRequest
	asyncRequest   *investapi.PostOrderAsyncRequest
	replaceRequest *investapi.ReplaceOrderRequest
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

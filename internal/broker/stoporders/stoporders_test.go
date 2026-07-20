package stoporders

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type recordingStopOrdersClient struct {
	investapi.StopOrdersServiceClient
	request *investapi.PostStopOrderRequest
}

func (c *recordingStopOrdersClient) PostStopOrder(
	_ context.Context,
	req *investapi.PostStopOrderRequest,
	_ ...grpc.CallOption,
) (*investapi.PostStopOrderResponse, error) {
	c.request = req
	return &investapi.PostStopOrderResponse{}, nil
}

func TestPlaceSetsTakeProfitFieldsOnlyForTakeProfitOrders(t *testing.T) {
	trailing := &TrailingParams{
		Indent:     &investapi.Quotation{Units: 1},
		IndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
		Spread:     &investapi.Quotation{Nano: 500_000_000},
		SpreadType: investapi.TrailingValueType_TRAILING_VALUE_RELATIVE,
	}
	tests := []struct {
		name             string
		stopOrderType    investapi.StopOrderType
		takeProfitType   investapi.TakeProfitType
		trailing         *TrailingParams
		wantExchange     investapi.ExchangeOrderType
		wantTakeProfit   investapi.TakeProfitType
		wantTrailingData bool
	}{
		{
			name: "regular take profit", stopOrderType: investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT,
			takeProfitType: investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR,
			wantExchange:   investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_MARKET,
			wantTakeProfit: investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR,
		},
		{
			name: "trailing take profit", stopOrderType: investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT,
			takeProfitType: investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING, trailing: trailing,
			wantExchange:   investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_MARKET,
			wantTakeProfit: investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING, wantTrailingData: true,
		},
		{
			name: "stop loss", stopOrderType: investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
			takeProfitType: investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR, trailing: trailing,
			wantTakeProfit: investapi.TakeProfitType_TAKE_PROFIT_TYPE_UNSPECIFIED,
		},
		{
			name: "stop limit", stopOrderType: investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT,
			takeProfitType: investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR, trailing: trailing,
			wantTakeProfit: investapi.TakeProfitType_TAKE_PROFIT_TYPE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &recordingStopOrdersClient{}
			client := Client{api: recorder}
			_, err := client.Place(context.Background(), PlaceParams{
				AccountID: "acc", InstrumentID: "uid", OrderID: "00000000-0000-4000-8000-000000000001",
				Direction:     investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
				StopOrderType: tt.stopOrderType, Quantity: 1,
				StopPrice:         &investapi.Quotation{Units: 100},
				ExchangeOrderType: investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_MARKET,
				TakeProfitType:    tt.takeProfitType, Trailing: tt.trailing,
			})
			if err != nil {
				t.Fatalf("Place: %v", err)
			}
			if got := recorder.request.GetTakeProfitType(); got != tt.wantTakeProfit {
				t.Errorf("take_profit_type = %v, want %v", got, tt.wantTakeProfit)
			}
			if got := recorder.request.GetExchangeOrderType(); got != tt.wantExchange {
				t.Errorf("exchange_order_type = %v, want %v", got, tt.wantExchange)
			}
			if got := recorder.request.GetTrailingData() != nil; got != tt.wantTrailingData {
				t.Errorf("trailing_data present = %v, want %v", got, tt.wantTrailingData)
			}
		})
	}
}

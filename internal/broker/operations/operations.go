// Package operations wraps cursor-paginated operation history reads.
package operations

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over GetOperationsByCursor.
type Client struct {
	api investapi.OperationsServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewOperationsServiceClient(cc)}
}

// ListParams describes one cursor query. All follows every returned cursor.
type ListParams struct {
	AccountID    string
	InstrumentID string
	From         *time.Time
	To           *time.Time
	Cursor       string
	Limit        int32
	All          bool
	State        investapi.OperationState
}

// Result contains the collected items and, for a single-page query, the
// cursor needed to continue it.
type Result struct {
	Items      []*investapi.OperationItem
	HasNext    bool
	NextCursor string
}

// ValidateLimit applies the broker's documented safe 3...1000 page-size range.
// The service accepts smaller pages, but documents duplicate and stalled
// cursor behavior when limit is not greater than two.
func ValidateLimit(limit int32) error {
	if limit < 3 || limit > 1000 {
		return fmt.Errorf("invalid operations limit %d: want 3 through 1000", limit)
	}
	return nil
}

// List fetches one page or follows cursors until exhausted when All is true.
func (c Client) List(ctx context.Context, params ListParams) (Result, error) {
	if err := ValidateLimit(params.Limit); err != nil {
		return Result{}, err
	}
	cursor := params.Cursor
	seenCursors := make(map[string]struct{})
	seenCursors[cursor] = struct{}{}
	items := make([]*investapi.OperationItem, 0)
	for {
		request := request(params, cursor)
		response, err := c.api.GetOperationsByCursor(ctx, request)
		if err != nil {
			return Result{}, err
		}
		items = append(items, response.GetItems()...)
		if !params.All {
			return Result{Items: items, HasNext: response.GetHasNext(), NextCursor: response.GetNextCursor()}, nil
		}
		if !response.GetHasNext() {
			return Result{Items: items}, nil
		}
		next := response.GetNextCursor()
		if next == "" {
			return Result{}, fmt.Errorf("operations pagination did not advance from cursor %q", cursor)
		}
		if _, seen := seenCursors[next]; seen {
			return Result{}, fmt.Errorf("operations pagination repeated cursor %q", next)
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
}

func request(params ListParams, cursor string) *investapi.GetOperationsByCursorRequest {
	request := &investapi.GetOperationsByCursorRequest{AccountId: params.AccountID}
	request.Limit = &params.Limit
	if params.InstrumentID != "" {
		request.InstrumentId = &params.InstrumentID
	}
	if params.From != nil {
		request.From = timestamppb.New(*params.From)
	}
	if params.To != nil {
		request.To = timestamppb.New(*params.To)
	}
	if cursor != "" {
		request.Cursor = &cursor
	}
	if params.State != investapi.OperationState_OPERATION_STATE_UNSPECIFIED {
		request.State = &params.State
	}
	return request
}

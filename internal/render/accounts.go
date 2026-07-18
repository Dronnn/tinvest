package render

import (
	"io"

	investapi "tinvest/internal/pb/investapi"
)

// AccountView is the JSON shape of one brokerage account. Enums keep their
// proto names, timestamps are RFC 3339 UTC (plan §7).
type AccountView struct {
	ID          string `json:"id"`
	Name        string `json:"name,omitempty"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	OpenedDate  string `json:"opened_date,omitempty"`
	ClosedDate  string `json:"closed_date,omitempty"`
	AccessLevel string `json:"access_level"`
}

// Account converts a proto account to its view.
func Account(a *investapi.Account) AccountView {
	return AccountView{
		ID:          a.GetId(),
		Name:        a.GetName(),
		Type:        a.GetType().String(),
		Status:      a.GetStatus().String(),
		OpenedDate:  Timestamp(a.GetOpenedDate()),
		ClosedDate:  Timestamp(a.GetClosedDate()),
		AccessLevel: a.GetAccessLevel().String(),
	}
}

// Accounts converts a proto account list, preserving order.
func Accounts(list []*investapi.Account) []AccountView {
	views := make([]AccountView, 0, len(list))
	for _, a := range list {
		views = append(views, Account(a))
	}
	return views
}

// AccountsTable renders the accounts list for humans.
func AccountsTable(w io.Writer, views []AccountView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.ID, v.Name, v.Type, v.Status, v.AccessLevel})
	}
	return Table(w, []string{"ID", "NAME", "TYPE", "STATUS", "ACCESS"}, rows)
}

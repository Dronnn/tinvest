package tinvest

import (
	"github.com/Dronnn/tinvest/internal/render"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// QuotationString renders a Quotation's exact units+nano pair as a decimal
// string ("1.5", "-0.001"), trimming trailing fractional zeros. It never uses
// floating point. A nil Quotation renders as "0".
func QuotationString(q *investapi.Quotation) string {
	return render.DecimalString(q.GetUnits(), q.GetNano())
}

// MoneyString renders a MoneyValue's exact units+nano pair as a decimal
// string, the same way QuotationString does; the currency is available
// separately via MoneyValue.GetCurrency. A nil MoneyValue renders as "0".
func MoneyString(m *investapi.MoneyValue) string {
	return render.DecimalString(m.GetUnits(), m.GetNano())
}

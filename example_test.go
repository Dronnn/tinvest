package tinvest_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Dronnn/tinvest"
)

// Example shows the read-only workflow: construct a client, resolve an
// instrument identifier, and read its last price. It is compiled to keep the
// documentation honest but is not run, since it needs a real token and
// network.
func Example() {
	ctx := context.Background()

	client, err := tinvest.New(ctx, tinvest.Config{
		Token:   "t.your_token_here",
		Timeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	// Identifiers may be an instrument_uid, a FIGI, or a TICKER@CLASSCODE pair.
	inst, err := client.Resolve(ctx, "SBER@TQBR")
	if err != nil {
		log.Fatal(err)
	}

	prices, err := client.LastPrices(ctx, inst.GetUid())
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range prices {
		fmt.Printf("%s: %s\n", inst.GetTicker(), tinvest.QuotationString(p.GetPrice()))
	}
}

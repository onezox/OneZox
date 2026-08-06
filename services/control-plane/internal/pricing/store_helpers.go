package pricing

import (
	"database/sql"
	"encoding/json"
	"errors"
)

func unitCostsJSON(u UnitCosts) []byte {
	b, err := json.Marshal(u)
	if err != nil {
		// UnitCosts is a fixed, fully-typed struct (two float64s and a
		// string) — Marshal cannot fail for it.
		panic("pricing: marshaling UnitCosts: " + err.Error())
	}
	return b
}

func unmarshalUnitCosts(data []byte, out *UnitCosts) error {
	return json.Unmarshal(data, out)
}

func scanEntry(row *sql.Row) (*Entry, error) {
	var (
		e        Entry
		costJSON []byte
	)
	if err := row.Scan(&e.PricingID, &e.ModelRef, &costJSON, &e.EffectiveAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := unmarshalUnitCosts(costJSON, &e.UnitCosts); err != nil {
		return nil, err
	}
	return &e, nil
}

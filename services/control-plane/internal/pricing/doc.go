// Package pricing implements per-model unit-cost reads/writes, backed by
// the pricing table (data/migrations/0011). Implementation lands in
// Phase-04 Step H, its own commit — this file exists only so the package
// compiles as part of control-plane's Step D skeleton.
//
// Scope note carried from the migration: this package provides pricing
// DATA only. It does not wire usage_event.usd_cost — that is a separate,
// conscious follow-on, not Phase-04 scope.
package pricing

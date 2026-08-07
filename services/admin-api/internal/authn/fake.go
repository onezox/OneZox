package authn

import "context"

// FakeStore holds rows keyed by hash — deliberately just a plain map with
// no notion of "which table" a row came from, which is exactly the point
// of TestApiKeyHashNeverAuthenticatesAsAdmin: this fake only ever contains
// what a test explicitly seeds into it (standing in for admin_user), so a
// hash that isn't seeded — even one that's a real, valid tenant api_keys
// hash in the real database — has nothing to match here, proving the
// lookup has no implicit fallback to any other credential space.
type FakeStore struct {
	rows map[string]*Row
	Err  error
}

func NewFakeStore() *FakeStore {
	return &FakeStore{rows: make(map[string]*Row)}
}

func (f *FakeStore) Seed(hash string, row *Row) {
	f.rows[hash] = row
}

func (f *FakeStore) LookupByHash(ctx context.Context, hash string) (*Row, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.rows[hash], nil
}

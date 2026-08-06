package providertoken

import "context"

// FakeSecretReader is an in-memory SecretReader for unit tests — no
// Vault needed.
type FakeSecretReader struct {
	Secrets map[string]string // provider -> api_key
}

func NewFakeSecretReader() *FakeSecretReader {
	return &FakeSecretReader{Secrets: make(map[string]string)}
}

func (f *FakeSecretReader) ReadProviderSecret(ctx context.Context, provider string) (string, error) {
	key, ok := f.Secrets[provider]
	if !ok {
		return "", errNotFound(provider)
	}
	return key, nil
}

type errNotFound string

func (e errNotFound) Error() string {
	return "no fake secret for provider: " + string(e)
}

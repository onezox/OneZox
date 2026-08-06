// Package registry implements the model manifest registry: signed,
// versioned, immutable manifests (RegisterModelManifest, GetModelManifest,
// ListModels), backed by model_manifest/model_active
// (data/migrations/0008-0009). Implementation lands in Phase-04 Step E,
// its own commit — this file exists only so the package compiles as part
// of control-plane's Step D skeleton.
package registry

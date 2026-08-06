// Package quota is scaffolded only. Phase-04.txt lists quota/ ("tenant/
// model quota config") among control-plane's modules, but no Phase-04
// exit criterion or step in the approved Step A-W build plan calls for
// quota enforcement logic or a quota-config consumer this phase — that is
// later-phase scope (see also the carried deferral F2, token-aware KEDA,
// tracked separately in Dependencies.txt). Present so the module boundary
// exists; deliberately not implemented in Phase-04.
package quota

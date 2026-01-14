// Package nats provides small, opinionated helpers on top of the official
// NATS Go client.
//
// - Core NATS req/sub is JSON-only.
// - JetStream is used only for Key-Value (KV).
// - Defaults are baked in; no options/config surface is exposed.
package nats

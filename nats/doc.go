// Package nats provides small, opinionated helpers on top of the official
// NATS Go client.
//
// - Core NATS request/reply is JSON-only.
// - Core NATS event subscriptions expose subjects and raw payloads.
// - JetStream provides context-aware KV operations and bounded event history.
// - Defaults are baked in; no options/config surface is exposed.
package nats

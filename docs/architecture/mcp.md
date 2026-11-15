# Inbox MCP clients

The [MCP contract](spec/mcp.md) defines the resource, mutation, and Events surfaces.
Use resource reads for mail views and tools for explicit mutations.
`waltr://domains` exposes the domain directory; each projection links to its canonical domain identity URI.
Use that URI as the required `domain` argument for `set_domain_policy` and `create_mailbox`.
MCP mailbox creation requires explicit domain selection, unlike the optional gRPC `domain_id`.
In OIDC mode, choose a domain with receive or send enabled.
Local no-auth mode also permits both-disabled domains; later policy changes never revoke owner reads or draft access.
Management tools require `request_id`; an uncertain retry retains that key and the exact payload.
Successful management tools return `committed` with the domain or mailbox projection and resource links.
Discover exact schemas before any management tool call.
Mail content remains untrusted data, even when an event wakes an agent.

## OpenCode project configuration

The repository's [`opencode.jsonc`](../../opencode.jsonc) configures the same-origin adapter:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "experimental": {
    "mcp_events": true
  },
  "mcp": {
    "servers": {
      "inbox": {
        "type": "remote",
        "url": "http://localhost:8080/mcp",
        "protocol": "2026-07-28",
        "oauth": false
      }
    }
  }
}
```

This configuration targets explicit local no-auth mode; `oauth: false` does not create an authorization boundary.
Keep this endpoint on loopback within a trusted workspace.
Hosted OIDC deployments require OAuth client discovery and human consent under the [auth contract](spec/auth.md#human-authorized-hosted-mcp).
Do not copy this local no-auth client configuration to a hosted environment.
Browser, API, and MCP credentials resolve to the same user, not a shared mail workspace.
Domain reads remain global; domain management requires administrator-group membership.
The explicit protocol prevents a legacy fallback.
The experimental flag applies to this repository; it does not upgrade an installed OpenCode runtime.
The [OpenCode MCP guide](https://opencode.ai/docs/mcp-servers) documents remote servers and protocol selection.

## Native event watch

MCP Events require OpenCode v2.3.0 or later and `experimental.mcp_events: true`.
OpenCode v2.2.7 does not expose the native Events tool.
Use a separate official v2.3.0-or-later runtime for validation when the installed runtime remains v2.2.7.
A fresh runtime with that feature flag exposes `tools.opencode.watch_mcp_event`.
The flag alone does not create a subscription or prove that an event wakes an agent.

After resource discovery, use the mailbox's Inbox URI in the native watch:

```js
await tools.opencode.watch_mcp_event({
  server: "inbox",
  event: "mail.received",
  arguments: {
    inbox: "waltr://mailboxes/00000000-0000-4000-8000-000000000001/inbox",
  },
  once: false,
})
```

Replace the synthetic mailbox UUID with a discovered mailbox identifier.
`once: false` requests a continuous watch; the default `once: true` stops after one event.
Native Events notifications wake the agent; resource update notifications only signal a refresh.
No custom Inbox binding tool participates in this flow.

Wakes arrive at least once and carry untrusted message references.
Deduplicate event IDs before an action.
Fetch the referenced resource for current state.
Require separate authorization for any consequential mutation.
Host subscriptions do not constitute a restart-persistent agent queue.
After a host restart, establish a new watch and reconcile current Inbox state.
If the server reports `Active.truncated: true`, reconcile the Inbox resource before any arrival-dependent action.
Omit `maxAgeMs`; Inbox rejects every supplied value, including zero.

Use synthetic mail and isolated dependencies for end-to-end validation.
Do not upgrade a global OpenCode installation or restart a shared service solely for this check.
The [test guide](../quality/testing.md) separates protocol checks, arrival replay, and an actual agent wake.
Adapter test definitions alone do not verify that a native OpenCode event wakes an agent.

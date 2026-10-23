# Running discovery

Discovery collects and caches runner inventories from orchestrators and other
discovery sources. It owns no runners and needs no runner or payment credentials.

Copy the [configuration example](../configs/discovery/config.example.toml), set
`Nodes` to your source base URLs, and run:

```sh
bin/livepeer discovery --config /etc/livepeer/discovery.toml
```

The standalone `bin/livepeer-discovery` accepts the same arguments.
Configuration follows the [shared conventions](configuration.md), with the
`LIVEPEER_DISCOVERY_` environment prefix and default directory
`~/.lpData/offchain/discovery`. `--print-config` prints the effective TOML settings.

## Sources and cache

Each source is queried at `<base URL>/discovery`, retaining any deployment path.
Duplicate URLs are skipped. Private sources require
`Grants`, such as `127.0.0.1:8935` for a local orchestrator. Redirects and URLs in
returned node metadata are not followed.

Refreshes run at startup and every `RefreshInterval` (default `5m`), with at most
five concurrent requests, a three-second timeout, and a 1 MiB limit per response.
HTTPS uses system trust. A successful response replaces that source's inventory,
including when empty. Failures retain the cached inventory and last successful
query time until `MaxFailedRefreshes` consecutive failures (default `6`). Set `0`
to evict on the first failure. Polling continues after eviction.

Keep the source topology acyclic. For example, if A queries an orchestrator and
B, while B queries A, both discovery services can keep circulating a runner
after its orchestrator withdraws it or goes offline. Peer queries still succeed,
so failure-based eviction does not remove those copies; `observed_at` is preserved
but has no age limit. This also applies to longer cycles and self-references.
Have discovery services query orchestrators directly, or arrange aggregation in
one direction with no path back to an earlier collector. Advertised node metadata
does not create these connections; only configured `Nodes` are queried.

## HTTP and advertising

The application listens on `127.0.0.1:8939` by default and serves `/discovery`
at the listener root. For public HTTPS, terminate TLS at a reverse proxy.
A proxy exposing a deployment prefix must strip it before forwarding.

`GET /discovery` returns cached runners grouped by their owning orchestrator.
`AdvertiseNodes` prepends a metadata-only entry when successfully queried sources
are available, listing their last successful query times. That entry has no `address`;
discovery has no public identity of its own. See the
[HTTP reference](http-api.md#discovery-http) for the format and duplicate runner
rules. `AdvertiseNodes` and `AdvertiseRunners` default to `true`; disabling either
omits the metadata entry or runner entries, respectively. Disabling both returns
`[]` and stops collection. Oversized responses are served with a warning, but
signers reject responses larger than 1 MiB.

The loopback metrics listener defaults to `127.0.0.1:8940` and serves `/healthz`,
`/readyz`, and `/metrics` (`livepeer_discovery_up`). Readiness indicates that the
service is running, even with no available sources. The cache is not persisted.

## Connect a signer

The signer's `Orchestrators` can include discovery and orchestrators:

```toml
Orchestrators = ["http://127.0.0.1:8939"]
DiscoveryGrants = ["127.0.0.1:8939"]
```

The signer's existing application and GPU filters apply. Returned runner URLs
continue to point to the owning orchestrator.

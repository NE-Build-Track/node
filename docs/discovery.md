# Running discovery

Discovery collects and caches runner inventories from orchestrators and other
discovery sources. It owns no runners and needs no runner or payment credentials.

Copy the [configuration example](../configs/discovery/config.example.toml), set
`Nodes` to your source base URLs, and run:

```sh
livepeer discovery --config /etc/livepeer/discovery.toml
```

`livepeer-discovery` accepts the same arguments. See
[shared configuration](configuration.md) for environment variables, paths, and
`--print-config`; the default directory is `~/.lpData/offchain/discovery`.

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

Keep configured `Nodes` acyclic, including no self-references. If A queries an
orchestrator and B, while B queries A, peers can circulate a withdrawn runner
indefinitely: peer queries succeed and preserved `observed_at` has no age limit.
Query orchestrators directly or aggregate in one direction. Advertised node
metadata creates no connections.

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

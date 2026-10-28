# CPS Auth

Read-only sensor authentication provider (`@scp/cps-auth`) used by the endpoint when `SENSOR_AUTH_STRATEGY=cps`.

A sensor is authorized when its public key is the identity account (`cps.identityNode`) of a CPS node that is a strict descendant of the `sensors.social` root node. The parent chain (`cps.nodes`) is walked until the root, a tree root, a cycle, or a hard step cap is reached.

## Caching

- Positive results: bounded LRU, no TTL (`CPS_AUTH_CACHE_MAX_SIZE`, default `100000`).
- Negative results: cached for `CPS_AUTH_NEGATIVE_TTL_SECONDS` (default `60`).
- RPC errors: not cached; access is denied (fail closed).
- Concurrent lookups for the same sensor share one chain query.

Changes to ancestry or removal of a previously authorized node are not observed until the entry is evicted or the service restarts.

## Configuration

See `services/endpoint/README.md` ("CPS Strategy Configuration"). `CPS_ROOT_ADDRESS` must resolve to an existing CPS root node at startup.

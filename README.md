# dosa-code-finder

A small Rust (axum) service. Every push to `main` builds the image once in GitHub Actions, publishes it as
`ghcr.io/hetp4401/dosa-code-finder:<commit>` (and `:latest`), and deploys it to the
[runner fleet](https://github.com/hetp4401/runner), where each machine just pulls the image.

- Live: https://dosa-1.billybishop4-workers.xyz (each replica *k* has its own URL, `dosa-<k>`; there's no shared one, and any of them starts or stops the scan)
- `compose.yaml` is what the fleet runs; `__TAG__` becomes the commit SHA at deploy time.
- The deploy step needs no secret: the fleet's apps are open to deploys.

Run locally: `docker compose -f compose.yaml up` after replacing `__TAG__` with `latest`, or `cargo run`.

## How the replicas work together

Every replica keeps a WebSocket to every other one (`wss://dosa-<k>.billybishop4-workers.xyz/mesh`; replica *i* dials
every *j > i*, pings every 20 s, reconnects on its own). Over it (`src/coord.rs`):

- **One scan at a time (a distributed lock).** Start proposes the scan to every replica; it begins only once a majority of
  `REPLICA_COUNT` (default 50, so 26) grants it. A replica grants one proposal at a time, and none while a scan runs or
  within the cooldown, so two scans can't both start. Every replica then knows the scan: any replica's page shows it.
- **All replicas at once, in a random order.** The replica it was started on coordinates: the range is shuffled (a
  permutation from the scan's seed), cut into chunks of 250, and every connected replica gets 2 chunks straight away and
  another each time it finishes one. A chunk not done in 3 minutes goes elsewhere and that replica rests 3 minutes.
- **Stop from anywhere.** Stop on any replica reaches every replica over the mesh, and over HTTP too (`/stop?local=1`).
  A code found anywhere stops every replica.
- **A coordinator that goes** (its machine leaves, say) is taken over after 20 s by the lowest replica connected, by the
  same majority vote; every replica heard every finished chunk, so it carries on with exactly what's left.
- **The cooldown** (`SCAN_COOLDOWN_HOURS`, 6 by default, 0 to turn it off): no scan starts within that long of the last
  one's start. The last scan and the codes found are passed to every replica that connects, so they outlive any one
  replica (not a restart of all of them at once).

`DOSA_MESH_KEY` (optional, the app's env): connections to `/mesh` without it are refused. `DOSA_TARGET_URL` stands in for
the site in tests.

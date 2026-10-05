# dosa-code-finder

A small Rust (axum) service. Every push to `main` builds the image once in GitHub Actions, publishes it as
`ghcr.io/hetp4401/dosa-code-finder:<commit>` (and `:latest`), and deploys it to the
[runner fleet](https://github.com/hetp4401/runner), where each machine just pulls the image.

- Live: https://dosa-code-finder-1.billybishop4-workers.xyz (each replica *k* has its own URL, `dosa-code-finder-<k>`; there's no shared one)
- `compose.yaml` is what the fleet runs; `__TAG__` becomes the commit SHA at deploy time.
- The deploy step needs the repo secret `FLEET_PASSWORD` (the fleet's password).

Run locally: `docker compose -f compose.yaml up` after replacing `__TAG__` with `latest`, or `cargo run`.

## The scan cooldown

Before a scan starts (`/start` or `/orchestrate/start`), the replica asks the fleet's key-value store, the `zkmetadata`
app, when the last scan started, whatever its range (key `dosa.lastrun`). Within `SCAN_COOLDOWN_HOURS` (6) of that,
the new scan is refused, with the last record in the answer. A store that has no record, or can't be reached, lets the
scan go. Each scan is recorded as the last one when it starts and when it ends (found code, tried, stopped), with the
password in the app's env `ZKMETADATA_PASSWORD`, which is what that key takes for changes. `ZKMETADATA_URLS`
(comma-separated) overrides the store's URLs; empty turns the check off.

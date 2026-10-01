# dosa-code-finder

A small Rust (axum) service. Every push to `main` builds the image once in GitHub Actions, publishes it as
`ghcr.io/hetp4401/dosa-code-finder:<commit>` (and `:latest`), and deploys it to the
[runner fleet](https://github.com/hetp4401/runner), where each machine just pulls the image.

- Live: https://dosa-code-finder.billybishop4-workers.xyz
- `compose.yaml` is what the fleet runs; `__TAG__` becomes the commit SHA at deploy time.
- The deploy step needs the repo secret `RUNNER_TOKEN` (the control plane's admin token).

Run locally: `docker compose -f compose.yaml up` after replacing `__TAG__` with `latest`, or `cargo run`.

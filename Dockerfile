FROM rust:1.88-slim AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/dosa-code-finder /usr/local/bin/dosa-code-finder
EXPOSE 3000
CMD ["dosa-code-finder"]


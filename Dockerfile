# Build
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

# Run — slim base, non-root, /data holds identity.key (mount + back it up).
FROM debian:bookworm-slim
RUN useradd --system --home /data seed && mkdir /data && chown seed /data
COPY --from=build /src/target/release/mstream-discovery-seed /usr/local/bin/
USER seed
VOLUME /data
ENTRYPOINT ["mstream-discovery-seed", "--data-dir", "/data"]

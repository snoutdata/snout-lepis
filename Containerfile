# snout-lepis's image: the router binary on a minimal glibc base, nothing else.
#
#   docker buildx build -f lepis/Containerfile --platform linux/arm64 -t snout-lepis .
#
# The context is the stack workspace root, for its lockfile (in the SnoutData monorepo,
# `packages/stack`; in snoutdata/snout-lepis, the repository root, with `-f Containerfile`).
#
# Not FROM scratch like the other servers: SQL is parsed by libpg_query (L4), whose build runs
# bindgen, and bindgen loads libclang at build time, which a static musl build script cannot do.
# So the binary links glibc and runs on distroless's `cc` base (glibc, no shell, no package
# manager). Node certificates are checked against the bundled web roots or LEPIS_HOME_CA.
FROM docker.io/library/rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
		clang libclang-dev protobuf-compiler \
	&& rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p snout-lepis \
	&& cp target/release/snout-lepis /snout-lepis

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /snout-lepis /snout-lepis
EXPOSE 5432
# How SnoutData Studio's "Find databases" knows this container is part of the SnoutData stack:
# by label, never by guessing from the image name.
LABEL com.snoutdata.stack="1" com.snoutdata.component="lepis"
ENTRYPOINT ["/snout-lepis"]

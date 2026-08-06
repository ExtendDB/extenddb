# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

# The runtime base is explicitly approved for this public OSS distribution.
# Keep both references digest-pinned and update them only through reviewed CRs.
FROM public.ecr.aws/docker/library/rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build
ARG VCS_REF
ARG BUILD_DATE
WORKDIR /src
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake=3.25.1-1 \
 && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN test -n "${VCS_REF}" \
 && test "${VCS_REF}" != unknown \
 && test -n "${BUILD_DATE}" \
 && test "${BUILD_DATE}" != unknown \
 && EXTENDDB_GIT_HASH="${VCS_REF}" \
    EXTENDDB_BUILD_TIME="${BUILD_DATE}" \
    cargo build --locked --release \
      -p extenddb --no-default-features --features postgres

FROM public.ecr.aws/debian/debian:bookworm-slim@sha256:1f6767130e3479e42348856acee11bbe78d26cc558b4bf52ac5106f3fcf594ff AS runtime
ARG VERSION
ARG VCS_REF
ARG BUILD_DATE
RUN test -n "${VERSION}" \
 && test -n "${VCS_REF}" \
 && test "${VCS_REF}" != unknown \
 && test -n "${BUILD_DATE}" \
 && test "${BUILD_DATE}" != unknown \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates=20230311+deb12u1 \
      tini=0.19.0-1+b3 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 extenddb \
 && useradd --uid 10001 --gid 10001 --create-home \
      --home-dir /var/lib/extenddb --shell /usr/sbin/nologin extenddb \
 && install -d -o 10001 -g 10001 /usr/share/doc/extenddb
# ExtendDB needs no setuid/setgid executable; remove those privilege paths.
RUN find / -xdev -type f -perm /6000 -exec chmod a-s {} +

COPY --from=build --chmod=0555 /src/target/release/extenddb /usr/local/bin/extenddb
COPY --chmod=0444 LICENSE NOTICE THIRD-PARTY-NOTICES.html /usr/share/doc/extenddb/

LABEL org.opencontainers.image.title="ExtendDB PostgreSQL backend" \
      org.opencontainers.image.description="DynamoDB-compatible API backed by an external PostgreSQL 14+ database" \
      org.opencontainers.image.source="https://github.com/ExtendDB/extenddb" \
      org.opencontainers.image.url="https://github.com/ExtendDB/extenddb" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV HOME=/var/lib/extenddb
WORKDIR /var/lib/extenddb
USER 10001:10001
EXPOSE 18443
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD ["extenddb", "healthcheck", "--config", "/var/lib/extenddb/extenddb.toml"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/extenddb"]
CMD ["serve", "--config", "/var/lib/extenddb/extenddb.toml", "--foreground"]

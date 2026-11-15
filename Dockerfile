# syntax=docker/dockerfile:1.7
FROM node:22.16.0-bookworm-slim@sha256:048ed02c5fd52e86fda6fbd2f6a76cf0d4492fd6c6fee9e2c463ed5108da0e34 AS web
WORKDIR /src
RUN npm install --global pnpm@10.32.1
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY web/package.json web/package.json
RUN pnpm install --frozen-lockfile
COPY web web
RUN pnpm web:build

FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS backend
ENV RUSTUP_TOOLCHAIN=1.96.0
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true \
    GIT_TERMINAL_PROMPT=0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=secret,id=gitconfig,target=/root/.gitconfig,mode=0400,required=false \
    --mount=type=secret,id=git-credentials,target=/root/.git-credentials,mode=0400,required=false \
    cargo build --locked --release -p inbox-server

FROM debian:bookworm-20260623-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
ARG REVISION
LABEL org.opencontainers.image.revision=$REVISION
RUN apt-get update && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 inbox \
    && useradd --uid 10001 --gid inbox --no-create-home --shell /usr/sbin/nologin inbox
WORKDIR /app
COPY --from=backend /src/target/release/inbox-server /usr/local/bin/inbox-server
COPY --from=web /src/web/dist /app/web
ENV INBOX_WEB_DIR=/app/web
USER 10001:10001
EXPOSE 8080 2525
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
    CMD curl --fail --silent --output /dev/null http://127.0.0.1:8080/ready || exit 1
ENTRYPOINT ["/usr/local/bin/inbox-server"]

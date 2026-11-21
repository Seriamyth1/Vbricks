# Shopping Sessions: native Rust pipeline. Built on the scorer (linux/amd64, Intel Xeon).
FROM rust:1.99.1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# AVX2 baseline: every Cascade Lake+ scorer has it.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
RUN chmod 755 /app/sessions
CMD ["/app/sessions"]

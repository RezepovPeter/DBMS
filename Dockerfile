FROM rust:1.81
WORKDIR /app
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src
RUN cargo build --release
CMD ["cargo run"]

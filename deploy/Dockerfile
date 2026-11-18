# ---- build stage ----
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake pkg-config \
      libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
      catch2 libgtest-dev libgmock-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build -j"$(nproc)" --target raft_server log_server test_raft_rpcs

# ---- runtime stage: just the programs and the libraries they need ----
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/raft_server /src/build/log_server /src/build/test_raft_rpcs /usr/local/bin/
WORKDIR /data

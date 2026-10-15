# Kafka-Inspired Message Broker

A C++17 systems project exploring persistent messaging, TCP protocol design, consumer coordination, and event-driven networking. The broker uses its own text protocol over length-prefixed TCP frames; it is not wire-compatible with Apache Kafka.

## Implementation

- Append-only partition logs with CRC32 record validation, `fsync`, and startup recovery that truncates incomplete or corrupt trailing records.
- `PRODUCE`, `FETCH`, `JOIN`, `GROUP_POLL`, `COMMIT`, and `LEAVE` commands with three partitions per topic.
- Producer and consumer clients; deterministic consumer-group partition assignment and in-memory committed offsets.
- Blocking thread-per-client and nonblocking Linux `epoll` server modes sharing the same broker logic.
- Simplified synchronous leader/follower replication with catch-up and manual promotion.
- Request counters, byte counts, latency percentiles, and recorded TCP-versus-epoll benchmark data.

## Architecture

`Client → TCP framing → protocol parser → broker → partition log`

The networking layer reconstructs complete frames and delegates parsed requests to the broker. Shared state is protected by mutexes; partition records are persisted on disk and loaded into memory during recovery. See [architecture](docs/architecture.md) and [storage semantics](docs/storage_recovery_semantics.md).

## Build and run

Requirements: Linux or WSL, a C++17 compiler, CMake 3.16+, and Python 3 for the benchmark tools. Linux `epoll` is required by the current build.

```bash
git clone https://github.com/kuvlay/kafka-broker.git
cd kafka-broker
cmake -S . -B build
cmake --build build
g++ -std=c++17 -Isrc clients/producer.cpp -o build/producer
g++ -std=c++17 -Isrc clients/consumer.cpp -o build/consumer
./build/kafka-broker 9092 data-local follower
```

In separate terminals:

```bash
./build/producer orders 0 "hello broker"
./build/consumer orders group-a consumer-1
```

Select the event-driven server with `./build/kafka-broker --epoll 9092 data-local follower`. For replication, start a follower on port 9093, then a leader with its follower port:

```bash
./build/kafka-broker 9093 data-follower follower
./build/kafka-broker 9092 data-leader leader 9093
```

## Verification and benchmarks

The repository contains C++ checks for record encoding, log storage, protocol parsing, replication, and metrics. After building, run the relevant targets directly:

```bash
./build/test_record
./build/test_topic_log
./build/test_protocol_parse
./build/test_broker_replicate
./build/test_stage11_metrics
```

The committed [benchmark data](benchmark-results/tcp_vs_epoll_concurrency.csv) records 30 runs across two server modes, 1–16 producers, 1,000 messages per producer, 1 KiB payloads, and three repetitions. Results are specific to that workload and machine; they are not a claim about production throughput. See [benchmark documentation](docs/stage11_observability.md).

## Scope and limitations

This is an educational implementation. Committed consumer offsets are not persisted across restarts. Replication has no quorum consensus, automatic leader election, automatic failover, or background retry loop. Synchronization is coarse-grained, topics use a fixed partition count, and benchmarks run on one machine. No exactly-once delivery guarantee is claimed.

## Provenance

Imported from [aarish-io/kafka-broker](https://github.com/aarish-io/kafka-broker) at the source owner's request for the [kuvlay portfolio](https://github.com/kuvlay). Original history and contributor attribution are retained. No new license is asserted.

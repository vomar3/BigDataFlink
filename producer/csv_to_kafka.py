import argparse
import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


DEFAULT_TOPIC = "mock_data"
DEFAULT_PARTITIONS = 4


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read mock_data CSV files row-by-row and send every row as JSON to Kafka."
    )
    parser.add_argument("--data-dir", default=os.getenv("CSV_DATA_DIR", "."))
    parser.add_argument("--bootstrap-server", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("KAFKA_PARTITIONS", DEFAULT_PARTITIONS)))
    parser.add_argument(
        "--key-mode",
        choices=["none", "constant", "id", "customer", "composite"],
        default=os.getenv("KEY_MODE", "none"),
        help="Kafka message key strategy for partition distribution experiments.",
    )
    parser.add_argument("--delay-ms", type=int, default=int(os.getenv("PRODUCER_DELAY_MS", "0")))
    return parser.parse_args()


def discover_csv_files(data_dir: Path):
    root_files = sorted(data_dir.glob("mock_data_*.csv"))
    if root_files:
        return root_files

    russian_dir = data_dir / "исходные данные"
    if russian_dir.exists():
        files = sorted(russian_dir.glob("MOCK_DATA*.csv"))
        if files:
            return files

    return [data_dir / "mock_data.csv"]


def wait_for_kafka(bootstrap_server: str, attempts: int = 30):
    last_error = None
    for _ in range(attempts):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_server, client_id="mock-data-admin")
            admin.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka is not available at {bootstrap_server}: {last_error}")


def ensure_topic(bootstrap_server: str, topic: str, partitions: int):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_server, client_id="mock-data-admin")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def normalize_row(row: dict, source_file: Path, source_line: int):
    normalized = {key: (value if value != "" else None) for key, value in row.items()}
    normalized["_source_file"] = source_file.name
    normalized["_source_line"] = source_line
    return normalized


def build_key(row: dict, mode: str):
    if mode == "none":
        return None
    if mode == "constant":
        return b"mock_data"
    if mode == "id":
        return str(row.get("id") or "").encode("utf-8")
    if mode == "customer":
        return str(row.get("sale_customer_id") or row.get("customer_email") or "").encode("utf-8")
    if mode == "composite":
        parts = [
            row.get("sale_customer_id") or "",
            row.get("sale_seller_id") or "",
            row.get("sale_product_id") or "",
        ]
        return ":".join(parts).encode("utf-8")
    raise ValueError(f"Unsupported key mode: {mode}")


def send_file(producer: KafkaProducer, topic: str, file_path: Path, key_mode: str, delay_ms: int):
    sent = 0
    with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for line_number, row in enumerate(reader, start=2):
            payload = normalize_row(row, file_path, line_number)
            producer.send(topic, key=build_key(payload, key_mode), value=payload)
            sent += 1
            if delay_ms > 0:
                time.sleep(delay_ms / 1000)
    producer.flush()
    print(f"Sent {sent} rows from {file_path.name}")
    return sent


def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    files = discover_csv_files(data_dir)
    if not files or not all(path.exists() for path in files):
        raise FileNotFoundError(f"No CSV files were found in {data_dir}")

    wait_for_kafka(args.bootstrap_server)
    ensure_topic(args.bootstrap_server, args.topic, args.partitions)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=10,
    )

    total = 0
    for file_path in files:
        total += send_file(producer, args.topic, file_path, args.key_mode, args.delay_ms)

    producer.close()
    print(f"Finished. Sent {total} JSON messages to topic '{args.topic}' with key-mode '{args.key_mode}'.")


if __name__ == "__main__":
    main()

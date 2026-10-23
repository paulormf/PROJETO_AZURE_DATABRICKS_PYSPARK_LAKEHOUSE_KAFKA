"""Publica eventos de teste no Amazon Kinesis Data Streams."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3


DEFAULT_STREAM_NAME = "lakehouse-sales-events"
DEFAULT_REGION = "us-east-1"


def build_sale_event(sequence: int) -> dict[str, object]:
    """Cria um evento de venda determinístico para teste."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "sale_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": 10000 + sequence,
        "sales_channel_id": (sequence % 3) + 1,
        "customer_id": 500 + sequence,
        "items": [
            {
                "product_id": (sequence % 10) + 1,
                "quantity": (sequence % 4) + 1,
                "unit_price": Decimal("49.90"),
            }
        ],
    }


def serialize_event(event: dict[str, object]) -> bytes:
    """Serializa o evento para JSON UTF-8, convertendo Decimal para número."""
    return json.dumps(
        event,
        ensure_ascii=False,
        default=lambda value: float(value)
        if isinstance(value, Decimal)
        else str(value),
    ).encode("utf-8")


def publish_events(
    stream_name: str,
    region: str,
    count: int,
    profile_name: str | None = None,
) -> int:
    """Publica eventos e retorna a quantidade confirmada pelo Kinesis."""
    session = boto3.Session(
        profile_name=profile_name,
        region_name=region,
    )
    client = session.client("kinesis")
    records = [
        {
            "Data": serialize_event(build_sale_event(sequence)),
            "PartitionKey": f"customer-{sequence}",
        }
        for sequence in range(1, count + 1)
    ]

    response = client.put_records(StreamName=stream_name, Records=records)
    failed_count = response.get("FailedRecordCount", 0)
    if failed_count:
        raise RuntimeError(
            f"Falha ao publicar {failed_count} de {count} eventos no Kinesis."
        )

    return len(records)


def parse_args() -> argparse.Namespace:
    """Lê os parâmetros da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Publica eventos de venda de teste no Amazon Kinesis."
    )
    parser.add_argument("--stream-name", default=DEFAULT_STREAM_NAME)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument(
        "--profile",
        dest="profile_name",
        help="Perfil AWS local; sem esse parâmetro usa a cadeia padrão da AWS.",
    )
    return parser.parse_args()


def main() -> None:
    """Executa a publicação de eventos de teste."""
    args = parse_args()
    if args.count < 1 or args.count > 500:
        raise ValueError("count deve estar entre 1 e 500.")

    published_count = publish_events(
        stream_name=args.stream_name,
        region=args.region,
        count=args.count,
        profile_name=args.profile_name,
    )
    print(
        f"Eventos publicados: {published_count}; "
        f"stream: {args.stream_name}; região: {args.region}"
    )


if __name__ == "__main__":
    main()

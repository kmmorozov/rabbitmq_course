import argparse
import json
import uuid
import pika
from common import connect

parser = argparse.ArgumentParser()
parser.add_argument("--exchange", default="app")
parser.add_argument("--key", default="orders.created")
parser.add_argument("--count", type=int, default=1)
parser.add_argument("--fail-until", type=int, default=0)
args = parser.parse_args()
connection = connect()
try:
    ch = connection.channel()
    ch.confirm_delivery()
    for number in range(args.count):
        message_id = str(uuid.uuid4())
        ch.basic_publish(
            exchange=args.exchange, routing_key=args.key, mandatory=True,
            body=json.dumps({"order_id": number, "fail_until": args.fail_until}).encode(),
            properties=pika.BasicProperties(
                content_type="application/json", delivery_mode=2,
                message_id=message_id, headers={"attempt": 0}),
        )
        print("CONFIRMED", message_id, flush=True)
finally:
    if connection.is_open:
        connection.close()

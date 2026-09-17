"""Create the isolated application topology; quorum version is created after joining."""
import argparse
from common import connect

parser = argparse.ArgumentParser()
parser.add_argument("--quorum", action="store_true")
args = parser.parse_args()
prefix = "ha" if args.quorum else "app"
connection = connect()
ch = connection.channel()
for name, kind in [(prefix, "topic"), (f"{prefix}.retry", "direct"),
                   (f"{prefix}.dlx", "direct")]:
    ch.exchange_declare(name, exchange_type=kind, durable=True)
queue_args = {"x-queue-type": "quorum" if args.quorum else "classic"}
if args.quorum:
    queue_args["x-quorum-initial-group-size"] = 3
ch.queue_declare(f"{prefix}.work", durable=True, arguments=queue_args)
ch.queue_bind(f"{prefix}.work", prefix, "orders.created")
ch.queue_declare(f"{prefix}.parking", durable=True, arguments=queue_args)
ch.queue_bind(f"{prefix}.parking", f"{prefix}.dlx", "failed")
for delay in [10, 60, 300]:
    retry_args = dict(queue_args, **{
        "x-message-ttl": delay * 1000,
        "x-dead-letter-exchange": prefix,
        "x-dead-letter-routing-key": "orders.created",
    })
    name = f"{prefix}.retry.{delay}s"
    ch.queue_declare(name, durable=True, arguments=retry_args)
    ch.queue_bind(name, f"{prefix}.retry", f"{delay}s")
connection.close()
print(f"Created {prefix}.work, {prefix}.parking and retry queues")

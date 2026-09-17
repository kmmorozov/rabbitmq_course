"""Manual ack, observable delays, optional bounded retry. Educational worker."""
import argparse
import json
import os
import pika
from common import connect

parser = argparse.ArgumentParser()
parser.add_argument("--prefix", default="app")
parser.add_argument("--prefetch", type=int, default=1)
parser.add_argument("--delay", type=float, default=0)
parser.add_argument("--retry", action="store_true")
parser.add_argument("--auto-ack", action="store_true")
args = parser.parse_args()
if args.retry and args.auto_ack:
    parser.error("retry requires manual acknowledgements")
if args.prefetch < 0 or args.delay < 0:
    parser.error("prefetch and delay must be nonnegative")

connection = connect()
ch = connection.channel()
ch.basic_qos(prefetch_count=args.prefetch)
ch.confirm_delivery()


def handle(channel, method, props, body):
    headers = dict(props.headers or {})
    attempt = int(headers.get("attempt", 0))
    print(f"RECEIVED pid={os.getpid()} id={props.message_id} attempt={attempt} "
          f"redelivered={method.redelivered}", flush=True)
    # Keeps processing I/O/heartbeats during a simulated delay.
    connection.sleep(args.delay)
    try:
        payload = json.loads(body)
        if attempt < int(payload.get("fail_until", 0)):
            raise ValueError("simulated downstream failure")
    except (ValueError, TypeError, AttributeError) as exc:
        if args.auto_ack:
            print(f"LOST: already auto-acked: {exc}", flush=True)
            return
        if not args.retry:
            channel.basic_nack(method.delivery_tag, requeue=False)
            print("NACK requeue=false", flush=True)
            return
        delays = [10, 60, 300]
        if attempt < len(delays):
            exchange, key = f"{args.prefix}.retry", f"{delays[attempt]}s"
        else:
            exchange, key = f"{args.prefix}.dlx", "failed"
        headers["attempt"] = attempt + 1
        headers["last-error"] = str(exc)
        try:
            channel.basic_publish(
                exchange=exchange, routing_key=key, body=body, mandatory=True,
                properties=pika.BasicProperties(
                    content_type=props.content_type, delivery_mode=2,
                    message_id=props.message_id, correlation_id=props.correlation_id,
                    headers=headers),
            )
        except pika.exceptions.AMQPError:
            # Do not ack the source on an uncertain publish. Closing requeues it.
            print("RETRY PUBLISH FAILED: original is not acknowledged", flush=True)
            raise
        channel.basic_ack(method.delivery_tag)
        print(f"FORWARDED {exchange}/{key} then ACK", flush=True)
    else:
        if not args.auto_ack:
            channel.basic_ack(method.delivery_tag)
        print("DONE", props.message_id, flush=True)


try:
    ch.basic_consume(f"{args.prefix}.work", handle, auto_ack=args.auto_ack)
    print(f"READY pid={os.getpid()} queue={args.prefix}.work", flush=True)
    ch.start_consuming()
except KeyboardInterrupt:
    if ch.is_open:
        ch.stop_consuming()
finally:
    if connection.is_open:
        connection.close()

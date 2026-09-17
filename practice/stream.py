import pika
from common import connect

connection = connect()
ch = connection.channel()
ch.queue_declare("lab.stream", durable=True, arguments={
    "x-queue-type": "stream", "x-max-age": "1h",
    "x-max-length-bytes": 100000000, "x-stream-max-segment-size-bytes": 10000000,
})
ch.confirm_delivery()
ch.basic_publish("", "lab.stream", b"stream demo", mandatory=True,
                 properties=pika.BasicProperties(delivery_mode=2))
ch.basic_qos(prefetch_count=10)

def receive(channel, method, props, body):
    print(props.headers, body, flush=True)
    channel.basic_ack(method.delivery_tag)

ch.basic_consume("lab.stream", receive, auto_ack=False,
                 arguments={"x-stream-offset": "first"})
print("Reading from first retained offset; Ctrl+C stops", flush=True)
try:
    ch.start_consuming()
except KeyboardInterrupt:
    ch.stop_consuming()
finally:
    connection.close()

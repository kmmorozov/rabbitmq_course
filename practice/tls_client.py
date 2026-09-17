import ssl
import pika

context = ssl.create_default_context(cafile="tls/ca.pem")
parameters = pika.ConnectionParameters(
    host="localhost", port=5691, virtual_host="/course",
    credentials=pika.PlainCredentials("admin", "admin-pass"),
    ssl_options=pika.SSLOptions(context, "localhost"),
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare("tls.demo", durable=True)
channel.confirm_delivery()
channel.basic_publish("", "tls.demo", b"verified TLS", mandatory=True,
                      properties=pika.BasicProperties(delivery_mode=2))
method, properties, body = channel.basic_get("tls.demo", auto_ack=False)
assert body == b"verified TLS", body
channel.basic_ack(method.delivery_tag)
connection.close()
print("TLS certificate and hostname verified; publish/get succeeded")

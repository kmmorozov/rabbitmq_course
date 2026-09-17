import os
import pika


def connect():
    params = pika.URLParameters(os.getenv(
        "AMQP_URL", "amqp://course:course-pass@localhost:5672/%2Fcourse"))
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    params.socket_timeout = 10
    params.connection_attempts = 3
    params.retry_delay = 2
    return pika.BlockingConnection(params)

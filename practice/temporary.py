from common import connect

connection = connect()
try:
    ch = connection.channel()
    result = ch.queue_declare(queue="", exclusive=True, auto_delete=True,
                              arguments={"x-queue-type": "classic"})
    print("Temporary queue:", result.method.queue, flush=True)
    input("Inspect it in the UI, then press Enter to close the connection: ")
finally:
    connection.close()

import time

from server import channel

QUEUE_LOGS = "logs-queue"
EXCHANGE_NAME = "direct-exchange-logs"

# create exchange
channel.exchange_declare(EXCHANGE_NAME, durable=True, exchange_type='direct')

# create a queue
channel.queue_declare(queue=QUEUE_LOGS)
channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_LOGS)

# publish event
#events = ["event 1", "event 2", "event 3", "event 4", "event 5"]

logs_files = open("/home/virus/Documents/GitHub/python/class-4/assets/web-server-nginx.log", "r")

for line in logs_files:
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=QUEUE_LOGS, body=line)
    time.sleep(2)
    print(f"[x] published {line}")
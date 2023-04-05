import time
import hashlib
import re

from ip2geotools.databases.noncommercial import DbIpCity
from pycountry import countries
from datetime import datetime, timedelta
import urllib.parse
from http.client import responses

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel



from models import Base,RowLog,CleanLog
from main import CreateEngine


def get_queue_length(queue_name):
    response = channel.queue_declare(queue=queue_name, passive=True)
    print(response.method.message_count)

get_queue_length("queue-data-lake")



''' Function with tqdm progress bar  when consuming messages from queue '''

def consume_queue(queue_name, callback, prefetch_count=1):
    channel.basic_qos(prefetch_count=prefetch_count)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()

def process_msg_data_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
    body = body.decode("utf-8")
    regex = r"\[.*?\]"
    hash_body = hashlib.md5(body.encode()).hexdigest()
    datetime = re.findall(regex,body)
    datetime = datetime[0].replace("[","").replace("]","")
    log = body
    print(hash_body)
    print(datetime)
    print(log)
    row_logs = RowLog(hash_body=hash_body,timestamp=datetime,log=log)
    con.add(row_logs)
    con.commit()
    print("Data inserted successfully")
    print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
    
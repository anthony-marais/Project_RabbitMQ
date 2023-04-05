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


from models import Base, RowLog, CleanLog
from main import CreateEngine


con = CreateEngine()


def process_msg_data_lake(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):

    body = body.decode("utf-8")
    regex = r"\[.*?\]"
    hash_body = hashlib.md5(body.encode()).hexdigest()
    datetime = re.findall(regex, body)
    datetime = datetime[0].replace("[", "").replace("]", "")
    log = body
    regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
    match = re.search(regex, body)

    user = match.group("user")
    session = match.group("session")

    if user != "-" and session != "-":

        print(hash_body)
        print(datetime)
        log = log.replace('-', "")
        log = log.replace('"', "")
        print(log)
        row_logs = RowLog(hash_body=hash_body, timestamp=datetime, log=log)
        con.add(row_logs)
        con.commit()
        print("Data inserted successfully")

        print(
            f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
    else:
        print("Message ignored")

channel.basic_consume(queue="queue-data-lake",
                      on_message_callback=process_msg_data_lake, auto_ack=True)
channel.start_consuming()
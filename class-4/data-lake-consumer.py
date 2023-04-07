import hashlib
import re

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel

from models import RowLog
from main import CreateEngine


con = CreateEngine()


def process_msg_data_lake(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):

    body = body.decode("utf-8")
    regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")

    def get_log(body):
        log = body
        log = log.replace('-', "")
        log = log.replace('"', "")
        return log

    def get_datetime(body):
        regex = r"\[.*?\]"
        datetime = re.findall(regex, body)
        datetime = datetime[0].replace("[", "").replace("]", "")
        return datetime

    def get_hash_body(body):
        hash_body = hashlib.md5(body.encode()).hexdigest()
        
        
        return hash_body    
    
    def get_match(regex, body):
            match = re.search(regex, body)
            if match:
                return match
            else:
                return None

    def get_user(match):
        user = match.group("user")
        if user == "-":
            return None
        return user
    
    def get_session(match):
        session = match.group("session")
        
        if session == "-":
            return None
        return session

    if get_match(regex, body) != None:
        if get_user(get_match(regex, body)) != "-" and get_session(get_match(regex, body)) != "-":
            
            
            row_logs = RowLog(hash_body= get_hash_body(body), timestamp=get_datetime(body), log=get_log(body))
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
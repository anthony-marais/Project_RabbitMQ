import time
import hashlib
import re

from ip2geotools.databases.noncommercial import DbIpCity
from pycountry import countries
from datetime import datetime, timedelta

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel



from models import Base,RowLog,CleanLog
from main import CreateEngine



session = CreateEngine()





def process_msg_data_lake(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):

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
    session.add(row_logs)
    session.commit()
    print("Data inserted successfully")

    print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")


def process_msg_data_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):



    body = body.decode("utf-8")
    regex = re.compile(r"(?P<ip>\S{7,15}) (?P<session>\S{1}|\S{15}) (?P<user>\S{1,50}) \[(?P<timestamp>\S{20}) "
                           r"(?P<utc>\S{5})\] \"(?P<method>GET|POST|DELETE|PATCH|PUT) (?P<url>\S{1,4096}) "
                           r"(?P<version>\S{1,10})\" (?P<status>\d{3}) (?P<size>\d+) -")
    match = re.search(regex,body)
    if match:
        timestamp = match.group("timestamp")
        format = "%d/%b/%Y:%H:%M:%S"
        date_time = datetime.strptime(timestamp, format)
        date_time = date_time + timedelta(hours=5)
        year = date_time.strftime("%Y")
        month = date_time.strftime("%m")
        day = date_time.strftime("%d")
        day_of_weak = date_time.strftime("%A")
        time = date_time.strftime("%H:%M:%S")
        ip_matches = match.group("ip")
        res = DbIpCity.get(ip_matches, api_key="free")
        country = str.upper(countries.get(alpha_2=res.country).name)
        city = str.upper(res.city)
        session = match.group("session")
        user = match.group("user")
        rest_method = match.group("method")
        url = match.group("url")
        
        version = match.group("version")
        status = match.group("status")
        size = match.group("size")

        print(timestamp)
        print(date_time)
        print(year)
        print(month)
        print(day)
        print(time)
        print(day_of_weak)
        print(country)
        print(city)
        print(session)
        print(user)
        print(rest_method)
        print(url)
        print(version)
        print(status)
        print(size)


        #clean_logs = CleanLog(ip=ip,timestamp=timestamp,utc=utc,method=method,url=url,version=version,status=status,size=size)




    #print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")




#def CallDataLakeQueue():
    # consume messages from queues
#channel.basic_consume(queue="queue-data-lake", on_message_callback=process_msg_data_lake, auto_ack=True)
#channel.start_consuming()
#CallDataLakeQueue()


#channel.basic_consume(queue="queue-data-lake", on_message_callback=process_msg_data_lake, auto_ack=True)
#channel.start_consuming()


channel.basic_consume(queue="queue-data-clean", on_message_callback=process_msg_data_clean, auto_ack=True)
channel.start_consuming()
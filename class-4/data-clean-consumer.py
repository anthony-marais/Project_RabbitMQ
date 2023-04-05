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


def process_msg_data_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):

    body = body.decode("utf-8")
    regex = re.compile(r"(?P<ip>\S{7,15}) (?P<session>\S{1}|\S{15}) (?P<user>\S{1,50}) \[(?P<timestamp>\S{20}) "
                       r"(?P<utc>\S{5})\] \"(?P<method>GET|POST|DELETE|PATCH|PUT) (?P<url>\S{1,4096}) "
                       r"(?P<version>\S{1,10})\" (?P<status>\d{3}) (?P<size>\d+) -")
    match = re.search(regex, body)
    if match:
        list_matches = []

        timestamp = match.group("timestamp")
        format = "%d/%b/%Y:%H:%M:%S"
        date_time = datetime.strptime(timestamp, format)
        date_time = date_time + timedelta(hours=5)
        year = date_time.strftime("%Y")
        month = date_time.strftime("%m")
        day = date_time.strftime("%d")
        day_of_week = date_time.strftime("%A")
        time = date_time.strftime("%H:%M:%S")
        ip_matches = match.group("ip")
        res = DbIpCity.get(ip_matches, api_key="free")
        country = str.upper(countries.get(alpha_2=res.country).name)
        city = str.upper(res.city)
        session = match.group("session")
        user = match.group("user")
        is_email = "True" if len(re.findall(
            r'[\w\.-]+@[\w\.-]+', user)) > 0 else "False"
        match_email_domain = re.findall(
            r'((?<=@)[^.]+(?=\.)+.+)', user) if is_email == "True" else "None"

        email_domain = match_email_domain[0] if type(
            match_email_domain) == list else match_email_domain
        rest_method = match.group("method")
        url = match.group("url")
        schema = urllib.parse.urlsplit(url).scheme
        host = urllib.parse.urlsplit(url).hostname

        version = match.group("version")
        status = match.group("status")
        status_verbose = responses[int(status)] if int(status) in responses else "None"

        size_bytes = match.group("size")
        size_kilo_bytes = round((int(size_bytes) / 1000), 2)
        size_mega_bytes = round((int(size_bytes) / 1000 / 1000), 2)

        list_matches.append([timestamp, year, month, day, day_of_week, time, ip_matches,
                             country, city, session, user, is_email, email_domain, rest_method, url, schema, host, version, status, status_verbose, size_bytes, size_kilo_bytes, size_mega_bytes])

        print(list_matches)
        # clean_logs = CleanLog(ip=ip,timestamp=timestamp,utc=utc,method=method,url=url,version=version,status=status,size=size)
        if user != '-' and session != '-':
            list_matches.append([timestamp, year, month, day, day_of_week, time, ip_matches,
                                 country, city, session, user, is_email, email_domain, rest_method, url, schema, host, version, status, status_verbose, size_bytes, size_kilo_bytes, size_mega_bytes])

            print(list_matches)
            clean_logs = CleanLog(timestamp=timestamp, year=year, month=month, day=day, day_of_week=day_of_week, time=time, ip=ip_matches, country=country, city=city, session=session, user=user, is_email=is_email, email_domain=email_domain,
                                  rest_method=rest_method, url=url, schema=schema, host=host, status=status, status_verbose=status_verbose, size_bytes=size_bytes, size_kilo_bytes=size_kilo_bytes, size_mega_bytes=size_mega_bytes)
            con.add(clean_logs)
            con.commit()
            print("Data inserted successfully")

            print(
                f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
        else:
            print("Message ignored")


channel.basic_consume(queue="queue-data-clean",
                      on_message_callback=process_msg_data_clean, auto_ack=True)
channel.start_consuming()
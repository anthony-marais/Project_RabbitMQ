import time
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


from models import CleanLog
from main import CreateEngine


con = CreateEngine()


def process_msg_data_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
    body = body.decode("utf-8")
    regex = re.compile(r"(?P<ip>\S{7,15}) (?P<session>\S{1}|\S{15}) (?P<user>\S{1,50}) \[(?P<timestamp>\S{20}) "
                       r"(?P<utc>\S{5})\] \"(?P<method>GET|POST|DELETE|PATCH|PUT) (?P<url>\S{1,4096}) "
                       r"(?P<version>\S{1,10})\" (?P<status>\d{3}) (?P<size>\d+) -")

    def match_regex(regex, body):
        match = re.search(regex, body)
        if match:
            return match
        else:
            return None

    match = match_regex(regex, body)

    if match:

        def get_timestamp(match):
            timestamp = match.group("timestamp")
            format = "%d/%b/%Y:%H:%M:%S"
            date_time = datetime.strptime(timestamp, format)
            date_time = date_time + timedelta(hours=5)
            return date_time

        def get_year(match):
            date_time = get_timestamp(match)
            year = date_time.strftime("%Y")
            return year

        def get_month(match):
            date_time = get_timestamp(match)
            month = date_time.strftime("%m")
            return month

        def get_day(match):
            date_time = get_timestamp(match)
            day = date_time.strftime("%d")
            return day

        def get_day_of_week(match):
            date_time = get_timestamp(match)
            day_of_week = date_time.strftime("%A")
            return day_of_week

        def get_time(match):
            date_time = get_timestamp(match)
            time = date_time.strftime("%H:%M:%S")
            return time

        def get_ip(match):
            ip_matches = match.group("ip")
            return ip_matches

        def get_country(ip_matches):
            res = DbIpCity.get(ip_matches, api_key="free")
            country = str.upper(countries.get(alpha_2=res.country).name)
            return country

        def get_city(ip_matches):
            res = DbIpCity.get(ip_matches, api_key="free")
            city = str.upper(res.city)
            return city

        def get_session(match):
            session = match.group("session")
            if session == "-":
                return "None"
            else:
                return session

        def get_user(match):
            user = match.group("user")
            if user == "-":
                return "None"
            else:
                return user

        def get_is_email(user):
            if "@" in user:
                return "True"
            else:
                return "False"

        def get_email_domain(user):
            if get_is_email(user) == "True":
                match_email_domain = re.findall(
                    r'((?<=@)[^.]+(?=\.)+.+)', user)
                return match_email_domain
            else:
                return "None"

        def get_rest_method(match):
            rest_method = match.group("method")
            return rest_method

        def get_url(match):
            url = match.group("url")
            return url

        def get_schema(url):
            schema = urllib.parse.urlsplit(url).scheme
            return schema

        def get_host(url):
            host = urllib.parse.urlsplit(url).hostname
            return host

        def get_version(match):
            version = match.group("version")
            return version

        def get_status(match):
            status = match.group("status")
            return status

        def get_status_verbose(match):
            status = match.group("status")
            status_verbose = responses[int(status)] if int(
                status) in responses else "None"
            return status_verbose

        def get_size(match):
            size = match.group("size")
            return size

        def get_size_kb(size):
            size_kb = int(size) / 1024
            return size_kb

        def get_size_mb(size):
            size_mb = int(size) / 1024 / 1024
            return size_mb

        if get_user(match) != "None" or get_session(match) != "None":
            
            clean_logs = CleanLog(timestamp=get_timestamp(match), year=get_year(match), month=get_month(match), day=get_day(match), day_of_week=get_day_of_week(match), time=get_time(match), ip=get_ip(match), country=get_country(get_ip(match)), city=get_city(get_ip(match)), session=get_session(match), user=get_user(match), is_email=get_is_email(get_user(match)), email_domain=get_email_domain(get_user(match)),
                                rest_method=get_rest_method(match), url=get_url(match), schema=get_schema(get_url(match)), host=get_host(get_url(match)), status=get_status(match), status_verbose=get_status_verbose(match), size_bytes=get_size(match), size_kilo_bytes=get_size_kb(get_size(match)), size_mega_bytes=get_size_mb(get_size(match)))
            con.add(clean_logs)
            con.commit()
            print("Data inserted successfully")

            print(
                f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
            time.sleep(0.05)

        else:
            print("Message ignored")


channel.basic_consume(queue="queue-data-clean",
                      on_message_callback=process_msg_data_clean, auto_ack=True,consumer_tag="consumer-data-clean")
channel.start_consuming()

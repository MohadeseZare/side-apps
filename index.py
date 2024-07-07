import time
import psycopg2
from datetime import datetime, timedelta
from psycopg2 import sql
from ast import literal_eval
import json
import pytz
from FCMmanager import sendNotification
from alarms import flow_limit, chamferAlarm
from schedule_arz import send_get_request

arz_counter = 0
while True:
    flow_limit()
    chamferAlarm()
    arz_counter += 1
    if (arz_counter == 24):
        arz_counter = 0
        send_get_request()
    #sendSMS('09135282058', "test")
    time.sleep(300)

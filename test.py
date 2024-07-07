import psycopg2
from datetime import datetime, timedelta
from kavenegar import *
import pytz


conn = psycopg2.connect(
database="gateway",
user='gatewayuser',
password='gateway123',
host='localhost',
port=''
)
print(1)
conn.autocommit = True
cursor = conn.cursor()
sql = ''' select * from logs_livedata; '''
cursor.execute(sql)
device_list = cursor.fetchall()
column_name = [desc[0] for desc in cursor.description]
for status_of_device in device_list:
    print(datetime.now(pytz.utc) - status_of_device[4])
    
    if datetime.now(pytz.utc) - status_of_device[4] >= timedelta(minutes=float(3)):
        sql = ''' delete from logs_livedata where mac_addr=%s and pin_id=%s and position=%s and type_data_id=%s; '''
        params = (status_of_device[3], status_of_device[8], status_of_device[7], str(status_of_device[6]))
        cursor.execute(sql, params)
        #all_device.remove(status_of_device)
        # restClient = Rest_Client('fazeljfz', 'fazel1377')
        # if float(['Value']) <= 20.0:
        #     text = ('شارژ پنل پیامکی تمام شده است')
            
        #     recId = restClient.SendSMS('09216315490', '50004000844811', text)
        #     print(recId)
        #     print(restClient.GetDeliveries2(recId))
        #     print(restClient.SendSMS('09389807463', '50004000844811', text))
        if status_of_device[7] == '1':
            text = ('مک آدرس ={} و پین= {} در ایدی={} دچار مشکل شده است'.format(status_of_device[3],
                                                                                status_of_device[8],
                                                                                status_of_device[7]))
            print(text)
        """ insert blank into rotations for use in reports in cut_off device"""
        rotations_sql = '''INSERT INTO logs_rotation  (mac_addr, pin_id, data, type_data_id, position, updated_at, "sendDataTime", diff_data, flag_on)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, null);'''
        params = (status_of_device[3], status_of_device[8], status_of_device[2], str(status_of_device[6]), status_of_device[7], status_of_device[1], datetime.now(pytz.utc), 0.0)
        print("this sql", rotations_sql, params)
        # cursor.execute(rotations_sql, params)
    else:
        pass
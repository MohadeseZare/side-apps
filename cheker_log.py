import time
import psycopg2
from datetime import datetime, timedelta
from kavenegar import *
from psycopg2 import sql
from ast import literal_eval
import json
import pytz
import schedule
from Rotations_scripts import rotations_main
from offline_data import sub_current_main
from charge_counts import main as charge_main

global all_device, packaging_device, stopage_counter
all_device = []
packaging_device = []
stopage_counter = 0
utc = pytz.UTC

def main():
    global all_device, packaging_device, stopage_counter
    # try:
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
        if datetime.now(pytz.utc) - status_of_device[4] >= timedelta(minutes=float(5)):
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
                sendSMS('09216315490', text)
            """ insert blank into rotations for use in reports in cut_off device"""
            rotations_sql = '''INSERT INTO logs_rotation  (mac_addr, pin_id, data, type_data_id, position, updated_at, "sendDataTime", diff_data, flag_on)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, null);'''
            params = (status_of_device[3], status_of_device[8], status_of_device[2], str(status_of_device[6]), status_of_device[7], status_of_device[1], datetime.now(pytz.utc), 0.0)
            print("this sql", rotations_sql, params)
            cursor.execute(rotations_sql, params)
        else:
            pass

    sql_packaging = ''' select * from packaging_packaginglivedata; '''
    cursor.execute(sql_packaging)
    device_list_packaging = cursor.fetchall()
    column_name = [desc[0] for desc in cursor.description]
    # for item in device_list_packaging:
        # if item not in packaging_device:
            # packaging_device.append(item)
        # else:
            # print(item[2])
    for status_of_device in device_list_packaging:
        #print(type(datetime.now(pytz.utc).timestamp()), datetime.fromtimestamp(status_of_device[2]).replace(tzinfo=utc),type(status_of_device[2]), timedelta(minutes=float(30)).total_seconds())
        if datetime.now(pytz.utc).timestamp() - float(status_of_device[2]) >= timedelta(minutes=float(70)).total_seconds():
            sql = ''' delete from packaging_packaginglivedata where mac_addr='{}'; '''.format(status_of_device[1])
            #params = (status_of_device[1])
            
            cursor.execute(sql)
            text = ('مک آدرس ={}در خط بسته بندی دچار مشکل شده است'.format(status_of_device[1]))
            sendSMS('09216315490', text)
        else:
            pass
    # cursor.close()
    stopage_counter = stopage_counter + 1
    #print(stopage_counter)
    if stopage_counter == 1:
        
        stoppage_time()
        stopage_counter = 0
        
    try:
        
        terminate_query = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'gateway' AND pid <> pg_backend_pid();"
        cursor.execute(terminate_query)

        conn.commit()

        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        # Close the cursor and the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def sendSMS(phone, text):

    try:
        api = KavenegarAPI('30464A49714E6A56396869736C565A4A764963637571354D76304773314A77467A6E4D54476138325541733D')
        params = {
            'sender': '2000550044',
            'receptor': phone,
            'message': text
        }   
        response = api.sms_send(params)
        print (str(response))
    except APIException as e: 
            print (str(e))
    except HTTPException as e: 
            print (str(e))

def stoppage_time():        
    source_conn = psycopg2.connect(database="gateway", user='gatewayuser', password='gateway123', host='localhost', port='')
    source_cursor = source_conn.cursor()
    with open('/home/rasam-user/Alarm/lastStoppageTime.txt', 'r') as file:
        mac_addrs = file.read()
    mac_addrs = literal_eval(mac_addrs)
    
    sections = ["Stacker", "Packaging machine"]
    error_section = {}
    for item in sections:
        source_cursor.execute(f"""select id from packaging_typeofalarm where section = '{item}';""")
        error_section[item] = source_cursor.fetchall()
        error_section[item] = [element for tuple_item in error_section[item] for element in tuple_item]
    for mac in mac_addrs.keys():
        while mac_addrs[mac] + 3600 <= datetime.now().timestamp():
            dur_start = mac_addrs[mac]
            dur_end = mac_addrs[mac] + 3600
            source_cursor.execute(f"""SELECT *
            FROM packaging_alarm
            WHERE mac_addr = '{mac}'
            and ((start_time BETWEEN {dur_start} AND {dur_end})
            OR (end_time BETWEEN {dur_end} AND {dur_end})
            OR (start_time < {dur_start} and end_time > {dur_end}));
            """)
            db_response = source_cursor.fetchall()
            for item in sections:
            
                if db_response!= []:
                    try:
                        for record in db_response:

                            start_time = 0
                            end_time = 0
                            if record[5] in error_section[item]: 
                                start_time = db_response[0][3]
                                if start_time < dur_start:
                                    start_time = dur_start
                                end_time = db_response[0][4]
                                if end_time > dur_end:
                                    end_time = dur_end
                                break
                        sum = 0
                        if start_time and end_time:    
                            for row in db_response:
                            
                                if row[5] in error_section[item]:
                                    row = list(row)
                                    row_start = row[3]
                                    if row_start < dur_start:
                                        row_start = dur_start
                                    row_end = row[4]
                                    if row_end > dur_end:
                                        row_end = dur_end
                                    
                                    if row_start >= start_time and row_end <= end_time:
                                        pass
                                    elif start_time <= row_start <= end_time < row_end:
                                        if row_end > dur_end:
                                            end_time = dur_end
                                        else:
                                            end_time = row_end
                                    elif row_start > end_time:
                                        sum += end_time - start_time
                                        start_time = row_start
                                        if row_end > dur_end:
                                            end_time = dur_end
                                        else:
                                            end_time = row_end
                                    elif row_start == dur_start and row_end == dur_end:
                                        
                                        sum = 3600
                                else:
                                    pass
                            if sum != 3600:
                                sum += end_time - start_time
                        source_cursor.execute(f"""INSERT INTO packaging_stoppagetime (mac_addr, dur_start, dur_end, dur_stoppage, section) VALUES (%s, %s, %s, %s, %s);""",[mac, dur_start, dur_end, sum, item])
                        source_conn.commit()
                       

                    except Exception as error:
                        pass
                        
                else:
                    mac_addrs[mac] = dur_end
            mac_addrs[mac] = dur_end
        with open('/home/rasam-user/Alarm/lastStoppageTime.txt', 'w') as file:
            file.write(str(mac_addrs))
            
    source_conn.close()

schedule.every(5).minutes.at(":00").do(main)
schedule.every(1).hour.at(":00").do(sub_current_main)
schedule.every(1).hours.at(":00").do(rotations_main)
schedule.every(8).hours.at(":00").do(charge_main)

while True:
    schedule.run_pending()
    time.sleep(1)
    # main()
    # time.sleep(300)
    

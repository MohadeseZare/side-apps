import psycopg2
from datetime import datetime, timedelta
import requests

def add_data_to_rotation_table(mac, pin, position, type_data_id, start_time, end_time):
    
    try:
        URL = "http://localhost:7000/gatewaya/gateway/api/getLogs/inPeriod/data/"
        BODY = {
            "start_time": start_time,
            "end_time": end_time,
            "mac_address": mac,
            "pin": pin,
            "position": position,
            "type_data": type_data_id,
            "report_id": "1"
        }
        logs_datas = requests.post(url=URL, data=BODY)
        print("logs return data:", logs_datas)
        logs_data = logs_datas.json()
        first_item = True
        LastCurrent = logs_data[0]['data']
        conn = psycopg2.connect(database="gateway", user='gatewayuser', password='gateway123', host='localhost', port='')
        cursor = conn.cursor()
        for item in logs_data:
            if first_item:
                first_item = False
                continue
            Current = item['data']
            if (LastCurrent <= 15 or Current <= 15) and not(LastCurrent <= 15 and Current <= 15):

                if LastCurrent <= 15:
                    query_temp = """insert into logs_rotation  (mac_addr, pin_id, position, type_data_id, data, "sendDataTime", diff_data, updated_at, flag_on) values (%s, %s, %s, %s, %s,%s, 0.0, %s, %s); """
                    query_param = (mac, pin, position, type_data_id, item['data'], item['sendDataTime'], item['sendDataTime'], "on")
                    cursor.execute(query_temp, query_param)
                if LastCurrent > 15:
                    query_temp = """insert into logs_rotation  (mac_addr, pin_id, position, type_data_id, data, "sendDataTime", diff_data, updated_at, flag_on) values (%s, %s, %s, %s, %s,%s, 0.0, %s, %s); """
                    query_param = (mac, pin, position, type_data_id, item['data'], item['sendDataTime'], item['sendDataTime'], "off")
                    cursor.execute(query_temp, query_param)

            LastCurrent = Current
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as error:
        print(error)
        pass


def delete_data_from_rotation_table(mac, pin, position, type_data_id, start_time):
    conn = psycopg2.connect(
    database="gateway",
    user='gatewayuser',
    password='gateway123',
    host='localhost',
    port=''
    )
    conn.autocommit = True
    cursor = conn.cursor()
    sql = '''delete from logs_rotation where mac_addr=%s and pin_id=%s and position=%s and type_data_id=%s and "sendDataTime">=%s '''
    params = (mac, pin, position, type_data_id, start_time)
    print(sql, params)
    cursor.execute(sql, params)
    cursor.close()
    conn.close()

def rotations_main():
    start_time = datetime.now() - timedelta(hours=24)
    end_time = datetime.now()
    delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("چمفر یک")
    delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("چمفر دو")
    delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="3", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="3", type_data_id="2", start_time=start_time, end_time=end_time)
    print("چمفر سه")
    delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="4", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="4", type_data_id="2", start_time=start_time, end_time=end_time)
    print("چمفر چهار")
    delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="0", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="0", type_data_id="2", start_time=start_time, end_time=end_time)
    print("چمفر ورودی")
    delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("1")
    delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("2")
    delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="3", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="3", type_data_id="2", start_time=start_time, end_time=end_time)
    print("3")
    delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="4", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="4", type_data_id="2", start_time=start_time, end_time=end_time)
    print("4")
    delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="5", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="5", type_data_id="2", start_time=start_time, end_time=end_time)
    print("5")
    delete_data_from_rotation_table(mac="ST:PB:01:02", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:01:02", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("6")
    delete_data_from_rotation_table(mac="ST:PB:01:02", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:01:02", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("7")
    delete_data_from_rotation_table(mac="ST:PB:01:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:01:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("8")
    delete_data_from_rotation_table(mac="ST:PB:01:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="ST:PB:01:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("9")
    print("shams")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("1")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("2")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="3", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="3", type_data_id="2", start_time=start_time, end_time=end_time)
    print("3")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="4", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="4", type_data_id="2", start_time=start_time, end_time=end_time)
    print("4")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="5", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="5", type_data_id="2", start_time=start_time, end_time=end_time)
    print("5")
    delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="6", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="6", type_data_id="2", start_time=start_time, end_time=end_time)
    print("6")
    delete_data_from_rotation_table(mac="SH:PB:02:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:02:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("7")
    delete_data_from_rotation_table(mac="SH:PB:02:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:02:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("8")
    delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="1", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="1", type_data_id="2", start_time=start_time, end_time=end_time)
    print("9")
    delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="2", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="2", type_data_id="2", start_time=start_time, end_time=end_time)
    print("10")
    delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="3", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="3", type_data_id="2", start_time=start_time, end_time=end_time)
    print("11")
    delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="4", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="4", type_data_id="2", start_time=start_time, end_time=end_time)
    print("12")
    delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="5", type_data_id="2", start_time=start_time)
    add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="5", type_data_id="2", start_time=start_time, end_time=end_time)
    print("13")
    

    print("successfully!!!")
    
    
# delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("چمفر یک")
# delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("چمفر دو")
# delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("چمفر سه")
# delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("چمفر چهار")
# delete_data_from_rotation_table(mac="ST:CH:01:01", pin=1, position="0", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:CH:01:01", pin=1, position="0", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("چمفر ورودی")
# delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("1")
# delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("2")
# delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("3")
# delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("4")
# delete_data_from_rotation_table(mac="ST:PB:02:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:02:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("5")
# delete_data_from_rotation_table(mac="ST:PB:01:02", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:01:02", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("6")
# delete_data_from_rotation_table(mac="ST:PB:01:02", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:01:02", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("7")
# delete_data_from_rotation_table(mac="ST:PB:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("8")
# delete_data_from_rotation_table(mac="ST:PB:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="ST:PB:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("9")
# print("shams")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("1")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("2")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("3")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("4")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("5")
# delete_data_from_rotation_table(mac="SH:PB:01:01", pin=1, position="6", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:01:01", pin=1, position="6", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("6")
# delete_data_from_rotation_table(mac="SH:PB:02:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:02:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("7")
# delete_data_from_rotation_table(mac="SH:PB:02:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:02:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("8")
# delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="1", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("9")
# delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="2", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("10")
# delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="3", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("11")
# delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="4", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("12")
# delete_data_from_rotation_table(mac="SH:PB:03:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00")
# add_data_to_rotation_table(mac="SH:PB:03:01", pin=1, position="5", type_data_id="2", start_time="2024-06-15 00:00:00", end_time="2024-06-15 10:00:00")
# print("13")


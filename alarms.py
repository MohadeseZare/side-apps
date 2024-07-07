import datetime
import requests
from kavenegar import *
import json

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
        
    
def requestLiveDevice():
    return requests.get("http://localhost:7000/gatewaya/gateway/livedata/").json()


def getDeviceDetail(mac, pin, position):
    data = requests.get(f"http://localhost:5000/factory/device/device_view/?mac_addr={mac}&pin={pin}&position={position}").json()
    return data['name'],data["product_line_part"]['name']


def checkFlow(max_usage, min_usage, liveData, mac, pin, position):
    if liveData != "no data" and  liveData >= 15.0:

        if liveData > max_usage:
            name,product_line_part = getDeviceDetail(mac, pin, position)
            message = "{name} از گروه {line} در جریان از حداکثر جریان که {max} بوده است عبور کرده و به جریان {now} رسیده است.".format(
                name=name, max=max_usage, now=liveData, line = product_line_part)
            return message
        elif liveData < min_usage:
            name, product_line_part = getDeviceDetail(mac, pin, position)
            message = " {name} از گروه {line} در جریان از حداقل جریان که {min} بوده است عبور کرده و به جریان {now} رسیده است.".format(
                name=name, min=min_usage, now=liveData, line= product_line_part)
            return message


def flow_limit():
    live = requestLiveDevice()
    for deviceLive in live:
        mac = deviceLive['mac_addr']
        pin = deviceLive['pin']
        position = deviceLive['position']
        liveData = deviceLive['data']
        message = ''
        
        # balmilKaf
        if mac == "ST:PB:01:01" and position == "1":
            message = checkFlow(270, 180, liveData, mac, pin, position)
        elif mac == "ST:PB:01:01" and position == "2":
            message = checkFlow(270, 180, liveData, mac, pin, position)
        elif mac == "ST:PB:01:02" and position == "1":
            message = checkFlow(270, 180, liveData, mac, pin, position)
        elif mac == "ST:PB:01:02" and position == "2":
            message = checkFlow(270, 180, liveData, mac, pin, position)

        # balmildivar
        elif mac == "ST:PB:02:01" and position == "1":
            message = checkFlow(180, 100, liveData, mac, pin, position)
        elif mac == "ST:PB:02:01" and position == "2":
            message = checkFlow(180, 100, liveData, mac, pin, position)
        elif mac == "ST:PB:02:01" and position == "3":
            message = checkFlow(180, 100, liveData, mac, pin, position)
        elif mac == "ST:PB:02:01" and position == "4":
            message = checkFlow(250, 145, liveData, mac, pin, position)
        elif mac == "ST:PB:02:01" and position == "5":
            message = checkFlow(250, 145, liveData, mac, pin, position)

        # champher
        elif mac == "ST:CH:01:01" and position == "1":
            message = checkFlow(500, 100, liveData, mac, pin, position)
        elif mac == "ST:CH:01:01" and position == "2":
            message = checkFlow(500, 100, liveData, mac, pin, position)
        elif mac == "ST:CH:01:01" and position == "3":
            message = checkFlow(500, 100, liveData, mac, pin, position)
        elif mac == "ST:CH:01:01" and position == "4":
            message = checkFlow(500, 100, liveData, mac, pin, position)

        if(message != '' and message is not None):
            print("in",message)
            #sendSMS("09216315490", message)
            sendSMS("09216315490", message)
            sendSMS("09134576360", message)
            #sendNotification
            #api for list 
        

def lastFiveMinLog(mac_address, pin, position, device_type_id):
    end_time = datetime.datetime.now() - datetime.timedelta(hours=3, minutes=30)
    start_time = end_time - datetime.timedelta(minutes=5)
    BODY = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "mac_address": mac_address,
        "pin": pin,
        "position": position,
        "type_data": device_type_id
    }
    return requests.post('http://localhost:7000/gatewaya/gateway/api/getLogs/inPeriod/data/', BODY).json()


def newList(lst):
    removeLst = []
    for index in range(len(lst)):
        if index == 0 or index == len(lst) - 1:
            removeLst.append(lst[index]['data'])
        else:
            if lst[index - 1]['data'] > lst[index]['data'] < lst[index + 1]['data'] or lst[index - 1]['data'] < \
                    lst[index]['data'] > lst[index + 1]['data']:
                removeLst.append(lst[index]['data'])
    return removeLst


def chamferAlarm():
    chamferList = ['ST:CH:01:01,1', 'ST:CH:01:01,2', 'ST:CH:01:01,3', 'ST:CH:01:01,4']
    for item in chamferList:

        mac_and_position = item.split(',')
        mac = mac_and_position[0]
        position = mac_and_position[1]
        alarmFlag = 0
        trueFlag = 0
        log = lastFiveMinLog(mac, 1, position, 2)
        newLst = newList(log)
        for index in range(len(newLst)):
            if index == len(newLst) - 1:
                pass
            else:
                if newLst[index] < 15:  # it's another alarm that should be handled.
                    alarmFlag = 0
                    trueFlag = 0
                diff = newLst[index] - newLst[index + 1]

                if -2.5 < diff < 2.5:
                    alarmFlag += 1
                elif diff > 50 or diff < -50:
                    alarmFlag = 0
                    trueFlag = 0
                else:
                    trueFlag += 1
            if alarmFlag == 15:
                name, product_line_part = getDeviceDetail(mac, 1, position)
                message = "{name} از گروه {line} دامنه نوسانات آن محدود گشته است.".format(
                    name=name, line=product_line_part)
                sendSMS("09216315490",message)
                break
            else:
                if trueFlag >= 5:
                    alarmFlag = 0
                    trueFlag = 0



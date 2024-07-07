import json
import time
import telepot
import requests


def send_get_request():
    url = "https://api.arzyab-crypto.com/api/ClientManager/Develop/TestServer/"
    while True:
        try:
            response = requests.get(url)
            response_data = response.json()

            if response_data != 'OK':
                error = "server"
                send_sms_message("server", "09904418840", error)
                send_sms_message("server", "09132511396", error)
                send_sms_message("server", "09133571085", error)

        except Exception as e:
            error_message = f"Error checking server status: {str(e)}"
            return error_message

        # time.sleep(2 * 60 * 60)


def send_sms_message(code, number, error):
    try:
        body = {
            "Mobile": number,
            "SmsCode": code,
            "AddName": error,
            "TemplateID": 1
        }
        # 6A0498CF-1248-441F-92FE-5FDEEA4141EC
        print(json.dumps(body), "call Body")
        headers = {"Authorization": "basic apikey:AD2B9307-B5C5-43E3-8D62-AEEC43F62F5B",
                   "Content-Type": "application/json"}
        res = requests.post(url="https://sms.parsgreen.ir/apiv2/Message/SendOTP", data=json.dumps(body),
                            headers=headers)
        print(res.status_code)
        print(res.content.decode('unicode-escape').encode('latin1').decode('utf-8'))
        return res
    except Exception as e:
        message_str = str(e)
        message_str = "send_sms_message - " + message_str
        return message_str


send_get_request()

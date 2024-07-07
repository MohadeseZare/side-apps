import requests
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from scipy.signal import butter, lfilter, decimate
from scipy import signal
import json
import psycopg2
from datetime import datetime, timedelta
import pytz, time


def fetch_data_from_api(mac_address, pin, position, type_data):
    local_tz = pytz.timezone('Etc/UTC')
    start_time = datetime.now(local_tz)
    end_time = start_time - timedelta(hours=8)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    data = {
        "start_time": end_time_str,
        "end_time": start_time_str,
        "mac_address": mac_address,
        "pin": pin,
        "position": position,
        "type_data": type_data,
    }

    url = 'http://45.139.10.137/gatewaya/gateway/api/getLogs/inPeriod/data/'
    response = requests.post(url, data=data)

    if response.status_code == 200:
        return response.json()
    else:
        return f"Request failed with status code: {response.status_code}"


def analyze_data(data):
    if isinstance(data, list) and data:
        timestamps = [entry['sendDataTime'] for entry in data]
        values = [entry['data'] for entry in data]
        df = pd.DataFrame({'Timestamp': timestamps, 'Value': values})
        df_len = len(df)
        xpoints = np.arange(df_len)
        ypoints = np.array(df['Value'])

        order = 3
        fs = df_len / 70
        cutoff = 0.5
        # plt.subplot(2, 1, 1)
        # plt.plot(xpoints, ypoints)

        y = butter_lowpass_filter(ypoints, cutoff, fs, order)

        # plt.subplot(2, 1, 2)
        # plt.plot(xpoints, y)

        q = 10
        samples_decimated = int(df_len / q) + 1
        ydem = decimate(y, q)
        duration = samples_decimated

        xnew = np.linspace(0, duration, samples_decimated, endpoint=False)

        dydt = np.gradient(y, xpoints)

        if np.max(dydt) != 0:
            dydt = dydt / np.max(dydt)
        else:
            # Handle the case where the maximum value is zero to avoid division by zero
            # For example, you can set dydt to all zeros or handle it in a way that fits your logic
            dydt = np.zeros_like(dydt)

        icl_u, ncl_u = find_cluster(dydt > 0.5, 1)
        upTrigger = xpoints[icl_u]
        startTriggers = len(upTrigger)

        icl_d, ncl_d = find_cluster(dydt < -0.5, 1)
        downTrigger = xpoints[icl_d]
        incomplete_end = False
        if data[-1]['data'] > 50:
            incomplete_end = True
            downTrigger = list(downTrigger)
            downTrigger.append(float(df_len - 1))

        data_info = {'upTriggers': list(upTrigger),
                     'downTriggers': list(downTrigger),
                     'incomplete_end': incomplete_end
                     }

        # plt.show()
        return data_info
    else:
        return "Invalid data format or empty data."


def butter_lowpass(cutoff, fs, order=5):
    return butter(order, cutoff, fs=fs, btype='low', analog=False)


def butter_lowpass_filter(data, cutoff, fs, order=5):
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = lfilter(b, a, data)
    return y


def find_cluster(x, xval):
    a = []
    kstart = -1
    kend = -1
    for i, xi in enumerate(x):
        if xi == xval:
            if kstart == -1:
                kstart = i
            if i == len(x) - 1:
                kend = i
        else:
            if kstart != -1 and kend == -1:
                kend = i - 1
        if kstart != -1 and kend != -1:
            a.append(kstart)
            a.append(kend)
            kstart = -1
            kend = -1
    i0 = a[0:-1:2]
    clustersize = list(np.array(a[1::2]) - np.array(i0) + 1)
    if not i0:
        i0 = []
        clustersize = []
    return i0, clustersize


def calculate_time_difference(time1, time2):
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    datetime1 = datetime.strptime(time1, time_format)
    datetime2 = datetime.strptime(time2, time_format)
    time_diff = datetime1 - datetime2
    return time_diff.total_seconds()


def charge_count(original_data, processed_data):
    all_charge = []
    for up, down in zip(processed_data['upTriggers'], processed_data['downTriggers']):
        up_timestamp = original_data[int(up)]
        down_timestamp = original_data[int(down)]
        charge = {
            'mac_address': up_timestamp['mac_addr'],
            'pin': up_timestamp['pin'],
            'position': up_timestamp['position'],
            'type_data_id': up_timestamp['type_data_id'],
            'charge_start_time': up_timestamp['sendDataTime'],
            'charge_end_time': down_timestamp['sendDataTime'],
            'incomplete_end': '',
            'complete_status': '',
            'stop_between_charge': ''
        }

        stop_between_charge = calculate_zero_intervals(original_data[int(up):int(down) + 1])
        time_differences = [calculate_time_difference(pair[1], pair[0]) for pair in stop_between_charge]

        total_time_seconds = sum(time_differences)
        charge['complete_status'] = total_time_seconds <= 5400
        charge['stop_between_charge'] = total_time_seconds
        charge['incomplete_end'] = down not in processed_data['downTriggers'] or up not in processed_data['upTriggers']

        if up == processed_data['upTriggers'][-1] and down == processed_data['downTriggers'][-1]:
            charge['incomplete_end'] = processed_data['incomplete_end']

        all_charge.append(charge)
    print(all_charge)
    return all_charge


def calculate_zero_intervals(data):
    zero_intervals = []
    zero_found = False
    start_time = None

    for idx, entry in enumerate(data):
        if entry['data'] == 0 and not zero_found:
            zero_found = True
            start_time = entry['sendDataTime']

        if entry['data'] != 0 and zero_found:
            zero_found = False
            zero_end_time = data[idx - 1]['sendDataTime']
            zero_intervals.append((start_time, zero_end_time))

    if zero_found:  # In case last entry is zero
        zero_intervals.append((start_time, data[-1]['sendDataTime']))

    return zero_intervals



def save_jsons(data):
    conn = psycopg2.connect(
        dbname="gateway",
        user="gatewayuser",
        password="gateway123",
        host="localhost",
        port=""
    )

    cur = conn.cursor()

    for charge in data:
        cur.execute(
            "INSERT INTO logs_chargecounts (mac_addr, pin_id, position, type_data_id, charge_start_time, "
            "charge_end_time, incomplete_end, complete_status, stop_between_charge) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
            (charge['mac_address'], charge['pin'], charge['position'], charge['type_data_id'],
             charge['charge_start_time'], charge['charge_end_time'], charge['incomplete_end'],
             charge['complete_status'], charge['stop_between_charge'])
        )

    conn.commit()
    cur.close()


def incomplete_end(charges):
    first_charge = charges[0]
    conn = psycopg2.connect(
        dbname="gateway",
        user="gatewayuser",
        password="gateway123",
        host="localhost"
    )
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND pin_id = %s AND incomplete_end = True",
        (first_charge['mac_address'], first_charge['position'], first_charge['type_data_id'], first_charge['pin']))

    row = cur.fetchone()  # Retrieve the first row

    if row:
        print(row)
        start_time = row[3]
        row_stop_between_charge = row[8]
        first_charge_stop_between_charge = first_charge['stop_between_charge']
        stop_between_charge = row_stop_between_charge + first_charge_stop_between_charge
        result = True
        if stop_between_charge >= 1800:
            result = False

        new_first_charge = {
            'mac_address': first_charge['mac_address'],
            'pin': first_charge['pin'],
            'position': first_charge['position'],
            'type_data_id': first_charge['type_data_id'],
            'charge_start_time': str(start_time),
            'charge_end_time': str(first_charge['charge_end_time']),
            'incomplete_end': False,
            'complete_status': result,
            'stop_between_charge': stop_between_charge
        }

        cur.execute("DELETE FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND pin_id = %s AND incomplete_end = True",
                    (first_charge['mac_address'], first_charge['position'], first_charge['type_data_id'], first_charge['pin']))

        conn.commit()

        charges[0] = new_first_charge
        return charges
    else:
        print("No result found for the query.")

    conn.close()


def main():
    device_infos = [
        {"mac_address": 'ST:PB:01:01', "pin": 1, "position": 1, "type_data": 2},
        {"mac_address": 'ST:PB:01:01', "pin": 1, "position": 2, "type_data": 2},
        {"mac_address": 'ST:PB:01:02', "pin": 1, "position": 1, "type_data": 2},
        {"mac_address": 'ST:PB:01:02', "pin": 1, "position": 2, "type_data": 2},
        {"mac_address": 'ST:PB:02:01', "pin": 1, "position": 1, "type_data": 2},
        {"mac_address": 'ST:PB:02:01', "pin": 1, "position": 2, "type_data": 2},
        {"mac_address": 'ST:PB:02:01', "pin": 1, "position": 3, "type_data": 2},
        {"mac_address": 'ST:PB:02:01', "pin": 1, "position": 4, "type_data": 2},
        {"mac_address": 'ST:PB:02:01', "pin": 1, "position": 5, "type_data": 2}
    ]

    for device_info in device_infos:
        api_data = fetch_data_from_api(device_info["mac_address"], device_info["pin"], device_info["position"],
                                       device_info["type_data"])
        if api_data:
            data = analyze_data(api_data)
            charges = charge_count(api_data, data)
            if charges:
                incomplete_end(charges)
                print(json.dumps(charges, indent=4))
                save_jsons(charges)
            else:
                print('no charges exist')


if __name__ == "__main__":
    main()
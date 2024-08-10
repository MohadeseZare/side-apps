import requests
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from scipy.signal import butter, lfilter, decimate
import json
import psycopg2
from datetime import datetime, timedelta
import pytz
import time


conn = psycopg2.connect(
    dbname="gateway",
    user="gatewayuser",
    password="gateway123",
    host="localhost",
    port=""
)
cur = conn.cursor()

TIME_FORMAT_API = "%Y-%m-%d %H:%M:%S%z"
TIME_FORMAT_DB = "%Y-%m-%dT%H:%M:%S%z"


def fetch_data_from_api(mac_address, pin, position, type_data):
    local_tz = pytz.timezone('Etc/UTC')
    now = datetime.now(local_tz)
    start_time = now - timedelta(hours=8)
    end_time = now
    start_time_str = start_time.strftime(TIME_FORMAT_API)
    end_time_str = end_time.strftime(TIME_FORMAT_API)
    data = {
        "start_time": start_time_str,
        "end_time": end_time_str,
        "mac_address": mac_address,
        "pin": pin,
        "position": position,
        "type_data": type_data
    }

    url = 'http://127.0.0.1:7000/gatewaya/gateway/api/getLogs/inPeriod/data/'
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
        print(f"df_len: {df_len}")
        order = 4
        fs = df_len / 73
        if fs < 2:
            fs =+ 2
        print(fs)
        cutoff = 1.5
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
        global last_data
        last_data = data[-1]
        if last_data['data'] > 50:
            incomplete_end = True
            downTrigger = list(downTrigger)
            downTrigger.append(float(df_len - 1))

        data_info = {'upTriggers': list(upTrigger),
                     'downTriggers': list(downTrigger),
                     'incomplete_end': incomplete_end
                     }

        # plt.show()
        # print(data_info)
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


def calculate_time_difference(time1, time2, time_format=TIME_FORMAT_DB):
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
        charge['complete_status'] = total_time_seconds <= 4800
        charge['stop_between_charge'] = total_time_seconds
        charge['incomplete_end'] = down not in processed_data['downTriggers'] or up not in processed_data['upTriggers']

        if up == processed_data['upTriggers'][-1] and down == processed_data['downTriggers'][-1]:
            charge['incomplete_end'] = processed_data['incomplete_end']

        all_charge.append(charge)
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


def save_jsons(charges):
    # print(charge['charge_start_time'])
    # print(charge['charge_end_time'])
    for charge in charges:

        cur.execute(
            "INSERT INTO logs_chargecounts (mac_addr, pin_id, position, type_data_id, charge_start_time, "
            "charge_end_time, incomplete_end, complete_status, stop_between_charge) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
            (charge['mac_address'], charge['pin'], charge['position'], charge['type_data_id'],
             charge['charge_start_time'], charge['charge_end_time'], charge['incomplete_end'],
             charge['complete_status'], charge['stop_between_charge'])
        )
    conn.commit()


def charge_start_time_status(charge):
    if charge['charge_start_time'] > charge['charge_end_time']:
        charge['charge_end_time'] = last_data['sendDataTime']


def check_charge_period_status(charge):
    cur.execute(
        "SELECT * FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND "
        "pin_id = %s ORDER BY charge_start_time DESC LIMIT 1;",
        (charge['mac_address'], charge['position'], charge['type_data_id'], charge['pin']))
    row = cur.fetchone()
    if row is not None:
        start_time_of_query = row[3]
        end_time_of_query = row[4]
        # print(end_time_of_query)
        charge_start_time = datetime.fromisoformat(charge['charge_start_time'].replace('Z', '+00:00'))
        charge_end_time = datetime.fromisoformat(charge['charge_end_time'].replace('Z', '+00:00'))
        # print(charge_start_time)
        time_difference = (charge_start_time - end_time_of_query).total_seconds()
        c = charge

        if time_difference <= 9600:
            c['charge_start_time'] = start_time_of_query
            c['charge_end_time'] = charge_end_time
            complete_time_difference = charge['stop_between_charge'] + row[-3] + time_difference
            c['stop_between_charge'] = complete_time_difference
            print(c['charge_end_time'])
            # end_time = datetime.fromisoformat(c['charge_end_time'].replace('T', ' '))
            print(c['charge_start_time'])
            charge_time = c['charge_end_time'] - c['charge_start_time']
            charge_time = charge_time.total_seconds()
            if complete_time_difference >= 4800:
                c['complete_status'] = False
            else:
                if charge_time <= 5400:
                    c['complete_status'] = False
                else:
                    c['complete_status'] = True
            cur.execute("DELETE FROM logs_chargecounts WHERE id = %s;", (row[0],))
        else:
            if row[-4] == False:
                cur.execute("DELETE FROM logs_chargecounts WHERE id = %s;", (row[0],))
    return charge


def difference_start_end(charge):
    result = calculate_time_difference(charge['charge_end_time'], charge['charge_start_time'])
    if result < 14400:
        charge['complete_status'] = False


def append_between_charges_together(charges):
    if charges:
        print(f"len charges: {len(charges)}")
        i = 1
        while i < len(charges) - 1:
            updated = False  # Flag to track if a charge was updated
            if not charges[i]['complete_status']:
                last_charge = charges[i - 1]
                next_charge = charges[i + 1]

                difference_last = (datetime.strptime(charges[i]['charge_start_time'], TIME_FORMAT_DB) -
                                   datetime.strptime(last_charge['charge_end_time'], TIME_FORMAT_DB))
                difference_next = (datetime.strptime(next_charge['charge_start_time'], TIME_FORMAT_DB) -
                                   datetime.strptime(charges[i]['charge_end_time'], TIME_FORMAT_DB))

                if difference_last < difference_next:
                    if difference_last.total_seconds() <= 5400:
                        charges[i]['charge_start_time'] = last_charge['charge_start_time']
                        complete_stop_between_charge = (last_charge['stop_between_charge'] +
                                                        charges[i]['stop_between_charge'] +
                                                        difference_last.total_seconds())
                        charges[i]['stop_between_charge'] = complete_stop_between_charge
                        charges[i]['incomplete_end'] = False

                        if (datetime.fromisoformat(str(charges[i]['charge_end_time'])) -
                            datetime.fromisoformat(str(charges[i]['charge_start_time']))).total_seconds() < 5400:
                            charges.remove(charges[i])
                        elif (5400 < (datetime.fromisoformat(str(charges[i]['charge_end_time'])) -
                                      datetime.fromisoformat(str(charges[i]['charge_start_time']))).total_seconds() <
                              16200):
                            charges[i]['complete_status'] = False
                        else:
                            if complete_stop_between_charge <= 5400:
                                charges[i]['complete_status'] = True
                            else:
                                charges[i]['complete_status'] = False
                        charges.remove(last_charge)
                        updated = True  # Charge was updated
                    else:
                        if ((datetime.strptime(charges[i]['charge_end_time'], TIME_FORMAT_DB) -
                             datetime.strptime(charges[i]["charge_start_time"], TIME_FORMAT_DB)).total_seconds()
                                >= 5400):
                            pass
                        else:
                            charges.remove(charges[i])
                elif difference_next < difference_last:
                    if difference_next.total_seconds() <= 5400:
                        next_charge['charge_start_time'] = charges[i]['charge_start_time']
                        complete_stop_between_charge = (charges[i]['stop_between_charge'] +
                                                        next_charge['stop_between_charge'] +
                                                        difference_next.total_seconds())
                        next_charge['stop_between_charge'] = complete_stop_between_charge

                        charge_end_time = datetime.strptime(next_charge['charge_end_time'], TIME_FORMAT_DB)
                        charge_start_time = datetime.strptime(next_charge['charge_start_time'], TIME_FORMAT_DB)
                        duration = charge_end_time - charge_start_time

                        if duration < timedelta(seconds=5400):
                            pass
                        elif timedelta(seconds=5400) < duration < timedelta(seconds=16200):
                            next_charge['complete_status'] = False
                        else:
                            if complete_stop_between_charge <= 5400:
                                next_charge['complete_status'] = True
                            else:
                                next_charge['complete_status'] = False
                        charges.remove(charges[i])
                        updated = True  # Charge was updated
                    else:
                        if ((datetime.strptime(charges[i]['charge_end_time'], TIME_FORMAT_DB) -
                                datetime.strptime(charges[i]["charge_start_time"], TIME_FORMAT_DB)).total_seconds()
                                <= 5400):
                            charges.remove(charges[i])
                        else:
                            pass
                    if ((datetime.fromisoformat(str(charges[i - 1]['charge_end_time'])) -
                            datetime.fromisoformat(str(charges[i - 1]["charge_start_time"]))).total_seconds() <= 5400):
                        charges.remove(charges[i - 1])
                    else:
                        pass

                else:
                    if (datetime.fromisoformat(str(charges[i - 1]['charge_end_time'])) -
                            datetime.fromisoformat(str(charges[i - 1]["charge_start_time"]))).total_seconds() <= 5400:
                        charges.remove(charges[i-1])
                    if (datetime.fromisoformat(str(charges[i]['charge_end_time'])) -
                            datetime.fromisoformat(str(charges[i]["charge_start_time"]))).total_seconds() <= 5400:
                        charges.remove(charges[i])

            if charges[i-1]['complete_status']:
                if (datetime.fromisoformat(str(charges[i-1]['charge_end_time'])) -
                        datetime.fromisoformat(str(charges[i-1]["charge_start_time"]))).total_seconds() <= 5400:
                    charges.remove(charges[i - 1])
            if updated:
                i = max(i - 1, 1)  # Ensure i doesn't go below 1
            else:
                i += 1  # Move to the next charge

        print(f"len(new_charges): {len(charges)}")
        return charges


def append_start_end_charges_together(charges):
    charge = charges[0]
    cur.execute(
        "SELECT * FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND "
        "pin_id = %s AND charge_start_time < %s AND charge_end_time < %s ORDER BY charge_end_time DESC LIMIT 2;",
        (
            charge['mac_address'],
            charge['position'],
            charge['type_data_id'],
            charge['pin'],
            charge['charge_start_time'],
            charge['charge_end_time']
        )
    )
    rows = cur.fetchall()

    if len(rows) < 1:
        return charges

    between_charge = list(rows[0])

    if len(rows) == 1:
        last_charge = between_charge
        end_time_of_last_charge = last_charge[4]
        start_time_of_between_charge = between_charge[3]
        end_time_of_between_charge = between_charge[4]
        difference_between_first_charge = (datetime.strptime(charge['charge_start_time'], TIME_FORMAT_DB) - end_time_of_between_charge)

        if between_charge[-5]:  # incomplete_end
            start_time = between_charge[3]
            row_stop_between_charge = between_charge[8]
            first_charge_stop_between_charge = charge['stop_between_charge']
            stop_between_charge = row_stop_between_charge + first_charge_stop_between_charge
            result = True
            if stop_between_charge >= 5400:
                result = False
            charge['charge_start_time'] = str(start_time)
            charge['incomplete_end'] = False
            charge['complete_status'] = result
            charge['stop_between_charge'] = stop_between_charge
            print(charge)
            cur.execute(
                "DELETE FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND pin_id = %s AND incomplete_end = True",
                (charge['mac_address'], charge['position'], charge['type_data_id'], charge['pin']))
        else:
            if difference_between_first_charge.total_seconds() <= 5400:
                charge['mac_address'] = between_charge[1]  # mac_addr
                charge['position'] = between_charge[2]  # position
                charge['charge_start_time'] = between_charge[3]  # charge_start_time
                charge['charge_end_time'] = charge['charge_end_time']  # charge_end_time
                charge['incomplete_end'] = between_charge[5]  # incomplete_end
                stop_between_charge = (between_charge[7] +
                                       charge['stop_between_charge'] +
                                       difference_between_first_charge.total_seconds())
                charge['stop_between_charge'] = stop_between_charge
                if (charge['charge_end_time'] - charge['charge_start_time']).total_seconds() >= 16200:
                    if stop_between_charge < 5400:
                        charge['complete_status'] = True
                    else:
                        charge['complete_status'] = False
                elif 5400 < (charge['charge_end_time'] - charge['charge_start_time']).total_seconds() < 16200:
                    charge['complete_status'] = False
                else:
                    charge['complete_status'] = False

                cur.execute(
                    "UPDATE logs_chargecounts "
                    "SET "
                    "charge_start_time = %s, charge_end_time = %s, "
                    "incomplete_end = %s, complete_status = %s, "
                    "stop_between_charge = %s "
                    "WHERE "
                    "charge_start_time = %s AND "
                    "charge_end_time = %s;",
                    (charge['charge_start_time'], charge['charge_end_time'], charge['incomplete_end'],
                     charge['complete_status'],
                     charge['charge_start_time'], charge['charge_end_time'])
                )
                cur.execute(
                    "DELETE FROM logs_chargecounts WHERE id = %s;",
                    (between_charge[0],)
                )
            else:
                if (not charge['complete_status'] and (datetime.fromisoformat(str(charge['charge_end_time'])) -
                                                       datetime.fromisoformat(str('charge_start_time'))).total_seconds()
                                                       < 5400):
                    cur.execute(
                        "DELETE FROM logs_chargecounts WHERE id = %s;",
                        (between_charge[0],)
                    )
                else:
                    pass

    elif len(rows) == 2:
        last_charge = list(rows[1])
        end_time_of_last_charge = last_charge[4]
        start_time_of_between_charge = between_charge[3]
        end_time_of_between_charge = between_charge[4]
        difference_last_between = start_time_of_between_charge - end_time_of_last_charge
        difference_between_first_charge = (datetime.strptime(charge['charge_start_time'], TIME_FORMAT_DB) - end_time_of_between_charge)

        if between_charge[-5]:  # incomplete_end
            start_time = between_charge[3]
            row_stop_between_charge = between_charge[7]
            first_charge_stop_between_charge = charge['stop_between_charge']
            stop_between_charge = row_stop_between_charge + first_charge_stop_between_charge
            result = True
            if stop_between_charge >= 5400:
                result = False
            charge['charge_start_time'] = str(start_time)
            charge['incomplete_end'] = False
            charge['complete_status'] = result
            charge['stop_between_charge'] = stop_between_charge
            print(charge)
            cur.execute(
                "DELETE FROM logs_chargecounts WHERE mac_addr = %s AND position = %s AND type_data_id = %s AND pin_id = %s AND incomplete_end = True",
                (charge['mac_address'], charge['position'], charge['type_data_id'], charge['pin']))
            conn.commit()

            # Comparing updated charge with last_charge
            print(charge['charge_start_time'])
            print(type(charge['charge_start_time']))
            print(last_charge[4])
            print(type(last_charge[4]))
            if (datetime.fromisoformat(charge['charge_start_time']) -
               datetime.fromisoformat(str(last_charge[4]))).total_seconds() <= 5400:
                charge['mac_address'] = last_charge[1]  # mac_addr
                charge['position'] = last_charge[2]  # position
                charge['charge_start_time'] = last_charge[3]  # charge_start_time
                charge['charge_end_time'] = charge['charge_end_time']  # charge_end_time
                charge['incomplete_end'] = last_charge[5]  # incomplete_end
                print(charge['charge_start_time'])
                print(last_charge[4])
                stop_between_charge = (last_charge[7] +
                                       charge['stop_between_charge'] +
                                       (datetime.fromisoformat(str(between_charge[3])) -
                                        datetime.fromisoformat(str(last_charge[4])))
                                       .total_seconds())
                charge['stop_between_charge'] = stop_between_charge
                if (datetime.fromisoformat(str(charge['charge_end_time'])) -
                    datetime.fromisoformat(str(charge['charge_start_time']))).total_seconds() >= 16200:
                    if stop_between_charge < 5400:
                        charge['complete_status'] = True
                    else:
                        charge['complete_status'] = False
                elif 5400 < (datetime.fromisoformat(str(charge['charge_end_time'])) -
                             datetime.fromisoformat(str(charge['charge_start_time']))).total_seconds() < 16200:
                    charge['complete_status'] = False
                else:
                    charge['complete_status'] = False
                print(charge)
                cur.execute(
                    "DELETE FROM logs_chargecounts WHERE id = %s;",
                    (last_charge[0],)
                )
            else:
                if ((datetime.fromisoformat(str(last_charge[4])) - datetime.fromisoformat(str(last_charge[3]))).
                        total_seconds() <=
                        5400):
                    cur.execute(
                        "DELETE FROM logs_chargecounts WHERE id = %s;",
                        (last_charge[0],)
                    )
                else:
                    pass
            conn.commit()
        else:
            if difference_last_between <= difference_between_first_charge:
                if difference_last_between.total_seconds() <= 5400:
                    print(between_charge)
                    print(last_charge)

                    between_charge[1] = last_charge[1]  # mac_addr
                    between_charge[2] = last_charge[2]  # position
                    between_charge[3] = last_charge[3]  # charge_start_time
                    between_charge[4] = between_charge[4]  # charge_end_time
                    between_charge[5] = between_charge[5]  # incomplete_end
                    stop_between_charge = last_charge[6] + between_charge[6] + difference_last_between.total_seconds()
                    between_charge[6] = stop_between_charge
                    if (datetime.fromisoformat(str(between_charge[4])) -
                            datetime.fromisoformat(str(between_charge[3]))).total_seconds() >= 16200:
                        if stop_between_charge < 5400:
                            between_charge[7] = True
                        else:
                            between_charge[7] = False
                    elif 5400 < (datetime.fromisoformat(str(between_charge[4])) -
                                 datetime.fromisoformat(str(between_charge[3]))).total_seconds() < 16200:
                        between_charge[7] = False
                    else:
                        between_charge[7] = False

                    cur.execute(
                        "UPDATE logs_chargecounts SET charge_start_time = %s, charge_end_time = %s, "
                        "incomplete_end = %s, complete_status = %s, stop_between_charge = %s WHERE id = %s;",
                        (between_charge[3], between_charge[4], between_charge[5], between_charge[7], between_charge[6],
                         between_charge[0])
                    )

                    cur.execute(
                        "DELETE FROM logs_chargecounts WHERE id = %s;",
                        (last_charge[0],)
                    )
                else:
                    if ((not last_charge[6]) and (datetime.fromisoformat(str(last_charge[4])) -
                                                  datetime.fromisoformat(str(last_charge[3]))).total_seconds()
                                                  < 5400):
                        cur.execute(
                            "DELETE FROM logs_chargecounts WHERE id = %s;",
                            (last_charge[0],)
                        )
                    if ((not between_charge[6]) and (datetime.fromisoformat(str(between_charge[4])) -
                                                     datetime.fromisoformat(str(between_charge[3]))).total_seconds()
                                                     < 5400):
                        cur.execute(
                            "DELETE FROM logs_chargecounts WHERE id = %s;",
                            (between_charge[0],)
                        )
            else:
                if difference_between_first_charge.total_seconds() <= 5400:
                    charge['mac_address'] = between_charge[1]  # mac_addr
                    charge['position'] = between_charge[2]  # position
                    charge['charge_start_time'] = between_charge[3]  # charge_start_time
                    charge['charge_end_time'] = charge['charge_end_time']  # charge_end_time
                    charge['incomplete_end'] = between_charge[5]  # incomplete_end
                    stop_between_charge = (between_charge[6] +
                                           charge['stop_between_charge'] +
                                           difference_between_first_charge.total_seconds())
                    charge['stop_between_charge'] = stop_between_charge
                    if (datetime.fromisoformat(str(charge['charge_end_time'])) -
                        datetime.fromisoformat(str(charge['charge_start_time']))).total_seconds() >= 16200:
                        if stop_between_charge < 5400:
                            charge['complete_status'] = True
                        else:
                            charge['complete_status'] = False
                    elif 5400 < (datetime.fromisoformat(str(charge['charge_end_time'])) -
                                 datetime.fromisoformat(str(charge['charge_start_time']))).total_seconds() < 16200:
                        charge['complete_status'] = False
                    else:
                        charge['complete_status'] = False
                    #
                    # cur.execute(
                    #     "UPDATE logs_chargecounts "
                    #     "SET "
                    #     "charge_start_time = %s, charge_end_time = %s, "
                    #     "incomplete_end = %s, complete_status = %s, "
                    #     "stop_between_charge = %s "
                    #     "WHERE "
                    #     "charge_start_time = %s AND "
                    #     "charge_end_time = %s;",
                    #     (charge['charge_start_time'], charge['charge_end_time'], charge['incomplete_end'],
                    #      charge['complete_status'],
                    #      charge['charge_start_time'], charge['charge_end_time'])
                    # )
                    cur.execute(
                        "DELETE FROM logs_chargecounts WHERE id = %s;",
                        (between_charge[0],)
                    )
                else:
                    if not charge['complete_status']:
                        cur.execute(
                            "DELETE FROM logs_chargecounts WHERE id = %s;",
                            (between_charge[0],)
                        )
                    else:
                        pass

    conn.commit()
    charges[0] = charge
    return charges


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
        # print(len(api_data))
        if api_data and (len(api_data) > 100):
            data = analyze_data(api_data)
            charges = charge_count(api_data, data)
            if charges:
                for charge in charges:
                    charge_start_time_status(charge)
                    difference_start_end(charge)
                    a = json.dumps(charge, indent=4)
                charges = append_start_end_charges_together(charges)
                charges = append_between_charges_together(charges)
                # print(charges)
                # print(f" chargessssss lennnnnnnn:\n {len(charges)}")
                # print(f" chargessssss: {charges}")
                save_jsons(charges)
            else:
                print('no charges exist')


conn.commit()

if __name__ == "__main__":
    main()
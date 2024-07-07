from datetime import datetime
import psycopg2



def read_file(path):
    with open(path, 'r') as file:
        null_subTable = eval(file.read())
        return null_subTable


def write_to_file(path, data):
    with open(path, 'w') as file:
        file.write(str(data))


class DB:
    def __init__(self):
        self.db_conn = psycopg2.connect(database="gateway", user="gatewayuser", password="gateway123", host="localhost",
                                        port="5432")
        self.db_cursor = self.db_conn.cursor()

    def get(self, start, end, mac, position):
        start_time = datetime.fromtimestamp(start)
        end_time = datetime.fromtimestamp(end)
        self.db_cursor.execute(
            f'''select * from logs_logdata 
            where mac_addr = '{mac}'
            and position = '{position}' 
            and "sendDataTime" between '{start_time}' and '{end_time}'
            order by "sendDataTime";''', [])
        response = self.db_cursor.fetchall()
        return response

    def write(self, period, record):
        tables = {
            60: 'logs_oneminflow',
            300: 'logs_fiveminflow',
            900: 'logs_fifteenminflow',
            1800: 'logs_thirtyminflow',
            3600: 'logs_onehourflow',
        }
        query_temp = f"""insert into {tables[period]} (mac_addr, pin_id, position, type_data_id, data, "sendDataTime", diff_data, updated_at) values (%s, %s, %s, %s, %s,%s, 0.0, %s); """
        query_params = (record[3], record[8], record[7], record[6], record[2], record[4], record[1])
        self.db_cursor.execute(query_temp, query_params)

    def commit(self):
        self.db_conn.commit()

    def close(self):
        self.db_cursor.close()


class GetEmptyRanges:
    def __init__(self):
        self.oneMinRange = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                            25, 26, 27, 28, 29, 30, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
                            58, 59]
        self.fiveMinRange = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        self.fifteenMinRange = [0, 15, 30, 45]
        self.thirtyMinRange = [0, 30]
        self.oneHourRange = [0]

    def emptySubTime(self, time, period):
        relative_time = time.minute
        if period == 60:
            if relative_time in self.oneMinRange:
                self.oneMinRange.remove(relative_time)
        elif period == 300:
            if relative_time in self.fiveMinRange:
                self.fiveMinRange.remove(relative_time)
        elif period == 900:
            if relative_time in self.fifteenMinRange:
                self.fifteenMinRange.remove(relative_time)
        elif period == 1800:
            if relative_time in self.thirtyMinRange:
                self.thirtyMinRange.remove(relative_time)
        elif period == 3600:
            if relative_time in self.oneHourRange:
                self.oneHourRange.remove(relative_time)

    def getList(self, start_range):
        for sub_index in range(len(self.oneMinRange)):
            self.oneMinRange[sub_index] += start_range
        for sub_index in range(len(self.fiveMinRange)):
            self.fiveMinRange[sub_index] += start_range
        for sub_index in range(len(self.fifteenMinRange)):
            self.fifteenMinRange[sub_index] += start_range
        for sub_index in range(len(self.thirtyMinRange)):
            self.thirtyMinRange[sub_index] += start_range
        for sub_index in range(len(self.oneHourRange)):
            self.oneHourRange[sub_index] += start_range
        return self.oneMinRange, self.fiveMinRange, self.fifteenMinRange, self.thirtyMinRange, self.oneHourRange


def get_sub_time(date_time, period):
    if period == 60:
        date_time = date_time.replace(second=0)
    elif period == 300:
        new_minute = (date_time.minute // 5) * 5
        date_time = date_time.replace(minute=new_minute, second=0)
    elif period == 900:
        new_minute = (date_time.minute // 15) * 15
        date_time = date_time.replace(minute=new_minute, second=0)
    elif period == 1800:
        new_minute = (date_time.minute // 30) * 30
        date_time = date_time.replace(minute=new_minute, second=0)
    elif period == 3600:
        date_time = date_time.replace(minute=0, second=0)
    return datetime.timestamp(date_time)


'''
null sub table
{
macaddres 1 : {60:[timestamp,timestamp], 300:[timestamp], 900:[],...},
macaddres 2 : {60:[timestamp], 300:[timestamp]},
}

last offline
{
macaddres 1 : timestamp,
macaddres 2 : timestamp,
}
'''


def sub_current_main():
    null_subTable = read_file('/home/rasam-user/Alarm/null_subTable.txt')
    db = DB()
    for macAndPosition in null_subTable.keys():
        mac_and_position = macAndPosition.split(',')
        mac = mac_and_position[0]
        position = mac_and_position[1]
        for period in null_subTable[macAndPosition].keys():
            if null_subTable[macAndPosition][period]:
                for time in null_subTable[macAndPosition][period]:
                    if datetime.timestamp(datetime.now()) - time >= 648000:
                        null_subTable[macAndPosition][period].remove(time)
                    else:
                        db_response = db.get(time, time + period, mac, position)
                        if db_response:
                            record = db_response[0]
                            db.write(period, record)
                            null_subTable[macAndPosition][period].remove(time)
    db.commit()
    write_to_file('/home/rasam-user/Alarm/null_subTable.txt', null_subTable)
    last_offline = read_file('/home/rasam-user/Alarm/last_offline.txt')
    for macAndPosition in last_offline.keys():
        mac_and_position = macAndPosition.split(',')
        mac = mac_and_position[0]
        position = mac_and_position[1]
        range_start = last_offline[macAndPosition]
        range_end = range_start + 3600
        last_one = -1
        last_five = -1
        last_fifteen = -1
        last_thirty = -1
        last_oneh = -1
        while range_end <= datetime.timestamp(datetime.now()):
            db_response = db.get(range_start, range_end, mac, position)
            sub_time = GetEmptyRanges()
            if db_response:
                for record in db_response:
                    start_one = get_sub_time(record[4], 60)
                    start_five = get_sub_time(record[4], 300)
                    start_fifteen = get_sub_time(record[4], 900)
                    start_thirty = get_sub_time(record[4], 1800)
                    start_oneh = get_sub_time(record[4], 3600)
                    if start_one != last_one:
                        db.write(60, record)
                        sub_time.emptySubTime(record[4], 60)
                        last_one = start_one
                    if start_five != last_five:
                        db.write(300, record)
                        sub_time.emptySubTime(record[4], 300)
                        last_five = start_five
                    if start_fifteen != last_fifteen:
                        db.write(900, record)
                        sub_time.emptySubTime(record[4], 900)
                        last_fifteen = start_fifteen
                    if start_thirty != last_thirty:
                        db.write(1800, record)
                        sub_time.emptySubTime(record[4], 1800)
                        last_thirty = start_thirty
                    if start_oneh != last_oneh:
                        db.write(3600, record)
                        sub_time.emptySubTime(record[4], 3600)
                        last_oneh = start_oneh
                db.commit()
            empty1, empty5, empty15, empty30, empty60 = sub_time.getList(range_start)
            range_start = range_end
            range_end += 3600
            
            null_subTable[macAndPosition][60] += empty1
            null_subTable[macAndPosition][300] += empty5
            null_subTable[macAndPosition][900] += empty15
            null_subTable[macAndPosition][1800] += empty30
            null_subTable[macAndPosition][3600] += empty60

        if range_start: 
            last_offline[macAndPosition] = range_start

    print("ok")
    write_to_file('/home/rasam-user/Alarm/null_subTable.txt', null_subTable)
    write_to_file('/home/rasam-user/Alarm/last_offline.txt', last_offline)
    db.close()





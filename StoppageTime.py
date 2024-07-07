import psycopg2
from datetime import datetime

source_conn = psycopg2.connect(database="gateway", user='gatewayuser', password='gateway123', host='localhost', port='')
source_cursor = source_conn.cursor()

mac_addrs= {'ST:PA:06:01': 1712174400}

sections = ["Stacker", "Packaging machine"]
error_section = {}
for item in sections:
    source_cursor.execute(f"""select id from packaging_typeofalarm where section = '{item}';""")
    error_section[item] = source_cursor.fetchall()
    error_section[item] = [element for tuple_item in error_section[item] for element in tuple_item]
for mac in mac_addrs.keys():
    while mac_addrs[mac] + 3600 <= datetime.now().timestamp() and mac_addrs[mac] < 1712520000:
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
import datetime
import socket
import os
import pickle
import time

from dateutil import rrule
from datetime import timedelta

from app.speed_layer.feed.load import all_data


# https://gist.github.com/jmhobbs/11276249

def get_data_frame(datetime, station):
    df = all_data
    print(f'Datetime : {str(datetime)} / Station : {str(station)}')

    df = df.filter((df['DATE'] == datetime) & (df['STATION'] == station))
    list_df = map(lambda row: row.asDict(), df.collect())

    return [row for row in list_df]


distinct_stations = all_data.select('STATION').distinct().rdd.map(lambda r: r[0]).collect()

print("Connecting...")
if os.path.exists("/tmp/python_unix_sockets_example"):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    client.connect("/tmp/python_unix_sockets_example")
    print("Ready.")
    print("Ctrl-C to quit.")

    from_date = datetime.datetime.strptime("01/01/20 00:00", "%d/%m/%y %H:%M")
    one_year_later = from_date + timedelta(days=365)

    try:
        # For every hour, and for every station
        # send corresponding record to socket
        for dt in rrule.rrule(rrule.HOURLY, dtstart=from_date, until=one_year_later):
            for station in distinct_stations:
                if station is None:
                    print('No station ! Skipping...')
                    break
                x = get_data_frame(dt, station)
                if "" != x and len(x) > 0:
                    data = pickle.dumps(x[0])
                    print("SENDING message to socket...")
                    client.send(data)
                    time.sleep(0.5)
                else:
                    print('No results found ! Skipping...')

    except KeyboardInterrupt as k:
        client.close()
        print("Shutting down.")
    except Exception as e:
        print('An error occured !')
        print(e)
else:
    print("Couldn't Connect!")
    print("Done")

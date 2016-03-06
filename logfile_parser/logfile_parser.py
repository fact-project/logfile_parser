import os
import re
import codecs

from collections import namedtuple
import pandas as pd

DateMessage = namedtuple("DateMessage", "severity time date mjd")
ServerMessage = namedtuple("ServerMessage", "severity time server state message")

def date_string_from_filename(path):
    basename = os.path.basename(path)
    y, m, d = basename[0:4], basename[4:6], basename[6:8]
    return "{0}-{1}-{2}".format(y, m, d)


re_date = re.compile("\s*(.)[>] (.*?) - [=]{10,} (.*) \[(\d*)\] [=]*")
re_server_message = re.compile(
    "\s*(?P<severity>.)[.\>]"
    " (?P<time>.*?)"
    " - (?P<server>.*?)"
    "(?P<state>_STATE\[-?\d*?\])*?"
    ":\s(?P<message>.*)")

def logfile2dataframe(path, ignore_client_lists=True):
    df = pd.DataFrame(server_messages(path))
    if not len(df):
        return df
    df["time"] = pd.to_datetime(df.time, errors="coerce")
    if ignore_client_lists:
        df = cut_client_lists(df)
    
    df["severity"] = df.severity.astype('category')
    df["server"] = df.server.astype('category')

    df = df.set_index("time")
    return df


def server_messages(path):
    last_seen_date = date_string_from_filename(path)
    server_messages = []
    with codecs.open(path, "r",encoding='utf-8', errors='ignore') as f:
        for line in f:
            try:
                m = re_server_message.match(line)
                server_message = ServerMessage(**m.groupdict())
                server_message = server_message._replace(
                    time=last_seen_date + ' ' + server_message.time)
                if not server_message.state is None:
                    server_message = server_message._replace(
                        state=int(server_message.state[7:-1]))
                server_messages.append(server_message)
            except AttributeError:
                pass

            try:
                last_seen_date = DateMessage(*re_date.match(line).groups()).date
            except AttributeError:
                pass

        return server_messages

def cut_client_lists(df):
    return df[~df.server.str.contains("CLIENT_LIST")]

def round_time(df, round_to_seconds=1.):
    round_to_nanoseconds = 1e9 * round_to_seconds
    df["rounded_time"] = pd.DatetimeIndex((df.index.asi8//round_to_nanoseconds).round() * round_to_nanoseconds)
    return df
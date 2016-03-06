import os
import re
import codecs

from collections import namedtuple
import pandas as pd

__all__ = ["logfile2dataframe"]

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
    "(?P<state>\[\d*?\])*?"
    ":\s(?P<message>.*)")

def logfile2dataframe(path):
    last_seen_date = date_from_filename(path)
    with codecs.open(path, "r",encoding='utf-8', errors='ignore') as f:
        for line in f:
            try:
                m = self.re_server_message.match(line)
                server_message = ServerMessage(**m.groupdict())
                server_message = server_message._replace(
                    time=last_seen_date + ' ' + server_message.time)
                log_messages.append(server_message)
            except AttributeError:
                pass

            try:
                last_seen_date = DateMessage(*self.re_date.match(line).groups()).date
            except AttributeError:
                pass

        df = pd.DataFrame(log_messages)
        if not len(df):
            return df
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        
        return df
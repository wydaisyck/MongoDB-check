import urllib.request as request
import datetime


def get_day_type(query_date):
    # 工作日对应结果为0, 休息日对应结果为1, 节假日对应的结果为2
    url = 'http://tool.bitefu.net/jiari/?d=' + query_date
    resp = request.urlopen(url)
    content = resp.read()
    if content:
        try:
            day_type = int(content)
        except ValueError:
            return -1
        else:
            return day_type
    else:
        return -1


def is_tradeday(query_date):
    weekday = datetime.datetime.strptime(query_date, '%Y%m%d').isoweekday()
    if weekday <= 5 and get_day_type(query_date) == 0:
        return 1
    else:
        return 0


def today_is_tradeday():
    query_date = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
    return is_tradeday(query_date)


def last_tradeday():
    # weekday = datetime.datetime.today().isoweekday()
    s = 0
    n = 1
    while s == 0:
        query_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=n), '%Y%m%d')
        # print(query_date)
        s = is_tradeday(query_date)
        n = n + 1
    query_date = query_date[0:4] + '-' + query_date[4:6] + '-' + query_date[6:8]
    return query_date


def last_tradeday2():
    # weekday = datetime.datetime.today().isoweekday()
    s = 0
    n = 1
    query_datei = "20210614"
    query_date = "20210614"
    while s == 0:
        query_date = str(int(query_datei)-n)
        # query_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=n), '%Y%m%d')
        print(query_date)
        s = is_tradeday(query_date)
        n = n + 1
    return query_date


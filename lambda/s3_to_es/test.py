import datetime

d = datetime.datetime.strptime("15/Jun/2016:19:50:46", '%d/%b/%Y:%H:%M:%S')
print datetime.date.strftime(d, '%Y-%m-%d %H:%M:%S')

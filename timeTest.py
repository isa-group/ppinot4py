import ppinot4py
from ppinot4py import model
import pandas as pd
import pm4py
from pm4py.objects.conversion.log import converter as log_converter

from business_duration import businessDuration
import holidays as pyholidays
from datetime import time
from itertools import repeat
import time as tmp

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

import datetime
import pytz
import businesstimedelta


start = tmp.time()

#Start date must be in standard python datetime format
start_date = pd.to_datetime('2006-07-24T00:00:00+02:00')
#Start date must be in standard python datetime format
end_date = pd.to_datetime('2006-12-05T00:00:00+01:00')
#Business open hour must be in standard python time format-Hour,Min,Sec
biz_open_time=time(7,0,0)
#Business close hour must be in standard python time format-Hour,Min,Sec
biz_close_time=time(17,0,0)
#US public holidays
US_holiday_list = pyholidays.ES(prov ='AN')
#Business duration can be 'day', 'hour', 'min', 'sec'
unit_hour='hour'

#Printing output
print(businessDuration(startdate=start_date,enddate=end_date,starttime=biz_open_time,endtime=biz_close_time,unit=unit_hour))


end = tmp.time()


print("The time of execution of above program is :", end-start)

start = tmp.time()
data = { 'start': ['2006-07-24T00:00:00+02:00'], 'end': ['2006-12-05T00:00:00+02:00'] }

df = pd.DataFrame(data)
df['start'] = pd.to_datetime(df['start'])
df['end'] = pd.to_datetime(df['end'])

us_bh = pd.tseries.offsets.CustomBusinessHour(calendar=USFederalHolidayCalendar())
df['count'] = df.apply(lambda x: len(pd.date_range(start=x.start, end=x.end, freq= us_bh)),axis=1)
end = tmp.time()

print("The time of execution of above program is :", end-start)


start = tmp.time()

# Define a working day
workday = businesstimedelta.WorkDayRule(
    start_time=datetime.time(9),
    end_time=datetime.time(18),
    working_days=[0, 1, 2, 3, 4])

# Take out the lunch break
lunchbreak = businesstimedelta.LunchTimeRule(
    start_time=datetime.time(12),
    end_time=datetime.time(13),
    working_days=[0, 1, 2, 3, 4])

# Combine the two
businesshrs = businesstimedelta.Rules([workday, lunchbreak])

starto = datetime.datetime(2006, 7, 24, 0, 0, 0)
endo = datetime.datetime(2006, 12, 5, 0, 0, 0)
bdiff = businesshrs.difference(starto, endo)


end = tmp.time()

print("The time of execution of above program is :", end-start)
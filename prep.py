import pandas as pd
import numpy as np
from math import cos, asin, sqrt, pi
import dask.dataframe as dd
import dask.array as da

pd.set_option('display.max_columns', None)

def distance(lat1, lon1, lat2, lon2):
    r = 6371
    p = pi / 180

    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 2 * r * asin(sqrt(a))


wdf = pd.read_csv('E:/bd_project/daily_weather_data_2020-2024.csv')
df = pd.read_parquet('E:/bd_project/general_travel_df_2020-2024.parquet')

wdf['date'] = pd.to_datetime(wdf['date'])
df['started_at'] = pd.to_datetime(df['started_at'])
df['ended_at'] = pd.to_datetime(df['ended_at'])

df['date_merge'] = df['started_at'].dt.date
wdf['date_merge'] = wdf['date'].dt.date
df = df.merge(wdf, how='inner', on='date_merge', sort=True)

weekends = {'01-01', '01-15', '02-12', '02-19', '03-04', '05-27', '06-19', '07-04', '09-02', '10-14', '11-11', '11-28', '12-25'}

df['day_name'] = df['started_at'].dt.day_name()
df['day'] = df['started_at'].dt.day
df['month'] = df['started_at'].dt.month
df['year'] = df['started_at'].dt.year
df['month_day'] = df['started_at'].dt.strftime('%m-%d')
df['year_month'] = df['started_at'].dt.strftime('%Y-%m')
df['year_month_day'] = df['started_at'].dt.strftime('%Y-%m-%d')
df['season'] = df['month'].apply(lambda month: month//3 + 1)
df['season_name'] = df['season'].replace({1:'Весна', 2:'Лето', 3:'Осень', 4:'Зима'})

df['is_weekend'] = df['day_name'].apply(lambda day: (day == 'Sunday' or day == 'Saturday'))
df['is_holiday'] = df['month_day'].apply(lambda md: (md in weekends))
df['is_weekend'] = df['is_weekend'].replace({True:1, False:0})
df['is_holiday'] = df['is_holiday'].replace({True:1, False:0})


df = dd.from_pandas(df, npartitions=100)

df['distance'] = df.apply(lambda row: distance(row['start_lat'], row['start_lng'], row['end_lat'], row['end_lng']), axis=1, meta=(None, 'float64'))

df['duration'] = df.apply(lambda row: (row['ended_at'] - row['started_at']), axis=1, meta=(None, 'timedelta64[ns]'))
df['duration_hours'] = df['duration'].dt.components.hours
df['duration_minutes'] = df['duration'].dt.components.minutes
df = df.assign(duration_minutes_total = lambda row: row.duration_hours * 60 + row.duration_minutes)
df['duration_seconds'] = df['duration'].dt.components.seconds

df['money'] = np.nan
df['money'] = df.apply(lambda row: max(0, row['duration_minutes_total'] - 45) * 0.18 if (row['member_casual'] == 'member' and row['rideable_type'] == 'classic_bike') else row['money'], axis=1, meta=(None, 'float64'))
df['money'] = df.apply(lambda row: row['duration_minutes_total'] * 0.18 if (row['member_casual'] == 'member' and row['rideable_type'] == 'electric_bike') else row['money'], axis=1, meta=(None, 'float64'))
df['money'] = df.apply(lambda row: row['duration_minutes_total'] * 0.29 if (row['member_casual'] == 'member' and row['rideable_type'] == 'scooter') else row['money'], axis=1, meta=(None, 'float64'))
df['money'] = df.apply(lambda row: row['duration_minutes_total'] * 0.18 + 1 if (row['member_casual'] == 'casual' and row['rideable_type'] == 'classic_bike') else row['money'], axis=1, meta=(None, 'float64'))
df['money'] = df.apply(lambda row: row['duration_minutes_total'] * 0.44 + 1 if (row['member_casual'] == 'casual' and row['rideable_type'] == 'electric_bike') else row['money'], axis=1, meta=(None, 'float64'))
df['money'] = df.apply(lambda row: row['duration_minutes_total'] * 0.44 + 1 if (row['member_casual'] == 'casual' and row['rideable_type'] == 'scooter') else row['money'], axis=1, meta=(None, 'float64'))

df = df.drop(columns=['sunrise', 'sunset', 'date', 'Unnamed: 0', 'date', 'date_merge'])

df = df.compute()

df.to_parquet('E:/bd_project/data.parquet')
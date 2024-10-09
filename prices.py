import pandas as pd
from math import ceil
import dask.dataframe as dd

df = pd.read_parquet('E:/bd_project/data.parquet')

df = df[df['rideable_type'] != 'docked_bike']
df['date'] = pd.to_datetime(df['year_month_day'])
df = df.set_index(df['date'])


df2 = df['2020-03-31':'2020-06-22']
df3 = df['2020-06-22':'2020-08-01']
df33 = df['2020-08-01':'2020-12-30']
df4 = df['2020-12-30':'2022-05-24']
df5 = df['2022-05-24':'2023-03-05']
df6 = df['2023-03-05':'2023-03-21']
df7 = df['2023-03-21':'2024-05-05']


df2 = dd.from_pandas(df2, npartitions=100)
df3 = dd.from_pandas(df3, npartitions=100)
df33 = dd.from_pandas(df33, npartitions=100)
df4 = dd.from_pandas(df4, npartitions=100)
df5 = dd.from_pandas(df5, npartitions=100)
df6 = dd.from_pandas(df6, npartitions=100)
df7 = dd.from_pandas(df7, npartitions=100)

'''
def price1(bike_type, member_type, duration):
    if member_type == 'casual':
        return 3 + 3 * ceil(max(duration - 30, 0) / 30)
    if member_type == 'member':
        return 3 * ceil(max(duration - 45, 0) / 30)
'''

def price2(bike_type, member_type, duration):
    if member_type == 'casual':
        return 1 + 3 * ceil(max(duration - 30, 0) / 30)
    if member_type == 'member':
        return 3 * ceil(max(duration - 45, 0) / 30)


def price3(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 3 + 0.15 * ceil(max(duration - 30, 0))
        if member_type == 'member':
            return 0.15 * ceil(max(duration - 45, 0))
    '''
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 3 + ceil(max(duration - 30, 0) * 0.2)
        if member_type == 'member':
            return 0.15 * ceil(max(duration - 45, 0))
    '''

def price33(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 3 + 0.15 * ceil(max(duration - 30, 0))
        if member_type == 'member':
            return 0.15 * ceil(max(duration - 45, 0))
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 3 + 0.15 * ceil(duration)
        if member_type == 'member':
            return 0.12 * ceil(max(duration, 0))

def price4(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 3.3 + 0.15 * ceil(max(duration - 30, 0))
        if member_type == 'member':
            return 0.15 * ceil(max(duration - 45, 0))
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 3.3 + 0.2 * ceil(max(duration - 15, 0))
        if member_type == 'member':
            return 0.12 * ceil(max(duration, 0))       


def price5(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 1 + 0.16 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.16 * ceil(max(duration - 45, 0))
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 1 + 0.39 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.16 * ceil(max(duration, 0))    


def price6(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 1 + 0.17 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.17 * ceil(max(duration - 45, 0))
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 1 + 0.42 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.17 * ceil(max(duration, 0))    

def price7(bike_type, member_type, duration):
    if bike_type == 'classic_bike':
        if member_type == 'casual':
            return 1 + 0.17 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.17 * ceil(max(duration - 45, 0))
    if bike_type == 'electric_bike':
        if member_type == 'casual':
            return 1 + 0.42 * ceil(max(duration, 0))
        if member_type == 'member':
            return 0.17 * ceil(max(duration, 0))  



df2['price'] = df2.apply(lambda x: price2(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df3['price'] = df3.apply(lambda x: price3(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df33['price'] = df33.apply(lambda x: price33(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df4['price'] = df4.apply(lambda x: price4(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df5['price'] = df5.apply(lambda x: price5(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df6['price'] = df6.apply(lambda x: price6(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))
df7['price'] = df7.apply(lambda x: price7(x['rideable_type'], x['member_casual'], x['duration_minutes_total']), axis=1, meta=(None, 'float64'))



df2 = df2.compute()
df3 = df3.compute()
df33 = df33.compute()
df4 = df4.compute()
df5 = df5.compute()
df6 = df6.compute()
df7 = df7.compute()


df2.to_parquet('E:/bd_project/df2.parquet')
df3.to_parquet('E:/bd_project/df3.parquet')
df33.to_parquet('E:/bd_project/df33.parquet')
df4.to_parquet('E:/bd_project/df4.parquet')
df5.to_parquet('E:/bd_project/df5.parquet')
df6.to_parquet('E:/bd_project/df6.parquet')
df7.to_parquet('E:/bd_project/df7.parquet')


df = pd.concat([df2, df3, df33, df4, df5, df6, df7])

df.to_parquet('E:/bd_project/data2.parquet')
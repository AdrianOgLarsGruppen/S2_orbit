import pandas as pd
import numpy as numpy
import re
import datetime
# import matplotlib.pyplot as plt
import sys


def fuso(lon):
    if lon < -180 or lon > 180:
        sys.exit('Error: longitude must be between -180 and 180.')
    elif lon < 0:
        timezone =  int(np.floor((lon + 7.5) / 15))
    elif lon >= 0:
        timezone = int(np.ceil((lon - 7.5) / 15))
    return timezone


# wf_s2_orb = '/data/andrelima/sentinel_2/data/'
wf_s2_orb = '/gpfs/glad1/Andre/data/sentinel2/orbit_data/'

df_s2_orb = pd.read_table(wf_s2_orb + 'S2A_relative_orbit_cicle_11days_01sec_RXXX.txt', sep=',')
# df_s2_orb = df_s2_orb.head(10000)

# Convert DATE and TIME string format to datetime format
df_s2_orb['TA_DATE2'] = pd.to_datetime(df_s2_orb.loc[:,'TA_DATE'])

# Zone Time zeroed Series
df_s2_orb['ZTIME'] = 0

# Looking for time zone and local time 
for idx in df_s2_orb.index.values:
    df_s2_orb.loc[idx,'ZTIME'] = fuso(df_s2_orb.loc[idx,'LONGITUDE'])
  
df_s2_orb['LOCALTIME'] = df_s2_orb.TA_DATE2 + pd.to_timedelta(df_s2_orb.ZTIME, unit='H')
# DataFrame filtered by Diurnal time orbit - between 09 and 11hs local time
# df_s2_orb_filt = df_s2_orb.query('(LTIME >= 9) & (LTIME <=11)')

# Setting time as an index and selecting data based on time interval
df_s2_orb_filt = df_s2_orb.set_index('LOCALTIME').between_time('08:45','12:15')

# Changing DataFrame index
df_s2_orb_filt['LOCALTIME'] = df_s2_orb_filt.index.values
df_s2_orb_filt = df_s2_orb_filt.set_index('FID')

# Parsing orbit name file name
df_s2_orb_filt['ORBIT_NAME'] = df_s2_orb_filt['Name'].str.extract('(\d+)', expand=True)
df_s2_orb_filt['ORBIT_NAME'] = 'R' + (df_s2_orb_filt.ORBIT_NAME.str.zfill(3))

# Creating empty DataFrame column to flag true orbit points (=1)
df_s2_orb_filt["FLAG_TRUE_ORBIT"] = 1

# Creating orbit array (ORBIT_NAME field) - for looping use
s2_orb_filt_arr = df_s2_orb_filt['ORBIT_NAME'].unique()
s2_orb_filt_arr.sort()

# Exclunding the wrong orbits points from STK11 orbits using the most frequent GMT day
for i in s2_orb_filt_arr:
    # Selecting data by orbit name
    df_s2_orb_temp = df_s2_orb_filt.loc[df_s2_orb_filt['ORBIT_NAME'] == i]
    # Selecting data not included in the most frequent date in the orbit selected before
    moda_date = df_s2_orb_temp.TA_DATE2.dt.date.mode()
    # Converting Series to DataFrame
    df_moda_date = pd.DataFrame({'MODA_DATE':moda_date.values})
    # Converting MODA_DATE data type to date/time format
    df_moda_date['MODA_DATE'] = pd.to_datetime(df_moda_date.loc[:,'MODA_DATE'])
    # Creating range of days to filter orbit points (plus 1 day, minus 1 day)
    df_moda_date['MODA_DATE_PLUS_1'] = pd.DatetimeIndex(df_moda_date.MODA_DATE)\
     + pd.DateOffset(1)
    df_moda_date['MODA_DATE_MINUS_1'] = pd.DatetimeIndex(df_moda_date.MODA_DATE)\
     + pd.DateOffset(-1)
    # Selecting point out of the days range considered
    df_s2_orb_temp = df_s2_orb_temp.loc[((df_s2_orb_temp['TA_DATE2'].dt.date >=\
     moda_date.MODA_DATE_MINUS_1.dt.date[0]) & (df_s2_orb_temp['TA_DATE2'].dt.date\
      <= moda_date.MODA_DATE_PLUS_1.dt.date[0])) == False]
    if len(df_s2_orb_temp) == 0 :
        continue
    df_s2_orb_temp["FLAG_TRUE_ORBIT"] = 0
    # Passing Flag 0 from a temporary DataFrame to a DataFrame taken as final result
    for j in df_s2_orb_temp.index.values:
        df_s2_orb_filt.loc[j,"FLAG_TRUE_ORBIT"] = df_s2_orb_temp.loc[j,"FLAG_TRUE_ORBIT"]
        print(j)

# Saving DataFrame to csv file
# df_s2_orb_filt.to_csv(wf_s2_orb + 'S2A_relative_orbit_cicle_11days_daytime_0845to1215h_01sec_RXXX.csv', sep=',', encoding='utf-8')

print('Fim')
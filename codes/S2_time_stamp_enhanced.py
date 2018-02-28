# Import Libraries
import pandas as pd
import numpy as np
from osgeo import gdal,ogr,osr
import datetime as dt
import glob
import os
import re
import sys

# # Functions
# To Timezone calculation
def fuso(lon):
    if lon < -180 or lon > 180:
        sys.exit('Error: longitude must be between -180 and 180.')
    elif lon < 0:
        timezone =  int(np.floor((lon + 7.5) / 15))
    elif lon >= 0:
        timezone = int(np.ceil((lon - 7.5) / 15))
    return timezone

# To calculate the time average
def avg_datetime(series):
	avg = (series.sum()/len(series))
	return avg

# To extraction image corners coordinates
def GetExtent(gt,cols,rows):
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
            #print x,y
        yarr.reverse()
    return ext

# To reproject image coordinates
def ReprojectCoords(coords,src_srs,tgt_srs):

    trans_coords=[]
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
        # print(x,y)
    return trans_coords

# To calculate time average
def avg_datetime(series):
    dt_min = series.min()
    deltas = [x-dt_min for x in series]
    return dt_min + functools.reduce(operator.add, deltas) / len(deltas)

# Working folders path
wf_s2_orb = '/gpfs/glad1/Andre/data/sentinel2/orbit_data/filtered_orbit/'
wf_s2_img = '/gpfs/glad1/Andre/data/af_s2_l8/sentinel2/fire_mask/'

# Listing Sentinel2 FILE_PATHs
s2_img_lst = glob.glob(wf_s2_img + 'S2A_MSIL1C*mastermask.TIF')
s2_img_files = [os.path.splitext(os.path.basename(fn))[0] for fn in s2_img_lst]

# Creating and Opening/reading DataFrames with Pandas
df_s2_orb = pd.read_csv(wf_s2_orb + 'S2A_relative_orbit_cicle_11days_daytime_20170901to20170911_0845to1215h_01sec_RXXX_points_data.csv', sep=',')
df_s2_img = pd.DataFrame({'FILE_PATH' : pd.Series(s2_img_lst),
    'FILE_NAME' : pd.Series(s2_img_files),})

# Parsing df_s2_img to extract time stamp
df_s2_img['TA_DATE'] = df_s2_img['FILE_PATH'].str.extract('(\d{8}.\d{6})', expand=True)

# Convert DATE and TIME string format to datetime format
df_s2_img['TA_DATE2'] = pd.to_datetime(df_s2_img['TA_DATE'])
df_s2_orb['TA_DATE2'] = pd.to_datetime(df_s2_orb['TA_DATE'])

# # Adding zone time Series to df_s2_orb dataframe
# df_s2_orb['ZTIME'] = 0

# # Calculating time zone
# for idx in df_s2_orb.index.values:
#     print(idx)
#     df_s2_orb.loc[idx,'ZTIME'] = fuso(df_s2_orb.iloc[idx,8])

# # Calculating local time
# df_s2_orb['LOCALTIME'] = df_s2_orb.TA_DATE2 + pd.to_timedelta(df_s2_orb.ZTIME, unit='H')

# # Filtering daytime orbit based on local time imagery interval
# df_s2_orb = df_s2_orb.set_index('LOCALTIME').between_time('9:30','11:30')

# # Parsing and formatting S2 orbit name, i.e. RXXX
# df_s2_orb['ORBIT_NAME'] = df_s2_orb['Name'].str.extract('(\d+)', expand=True)
# df_s2_orb['ORBIT_NAME'] = 'R' + (df_s2_orb.ORBIT_NAME.str.zfill(3))

# Convert DATE and TIME object to datetime format
df_s2_orb['LOCALTIME'] = pd.to_datetime(df_s2_orb['LOCALTIME'])

# Parsing df_s2_img to extract orbit name
df_s2_img['ORBIT_NAME'] = df_s2_img['FILE_PATH'].str.extract('\d{8}.\d{6}.\w{6}(\w{4})', expand=True)

# Extracting corners coordinates from AF_mask to df_s2_img
for i in df_s2_img.index.values:

    raster = df_s2_img.iloc[i,1]
    ds = gdal.Open(raster)

    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    ext = GetExtent(gt,cols,rows)

    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())
    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(4326)
    tgt_srs = src_srs.CloneGeogCS()

    geo_ext = ReprojectCoords(ext,src_srs,tgt_srs)

    df_s2_img.loc[i, 'LAT_IMG_MAX'] = geo_ext[0][1]
    df_s2_img.loc[i, 'LAT_IMG_MIN'] = geo_ext[2][1]
    df_s2_img.loc[i, 'LON_IMG_MAX'] = geo_ext[3][0]
    df_s2_img.loc[i, 'LON_IMG_MIN'] = geo_ext[1][0]

     
# Rescaling Longitude values from -180 -- 180 to 0 -- 360
# df_s2_img_long = df_s2_img.filter(regex='LON_IMG').columns
# df_s2_img[df_s2_img_long] = df_s2_img[df_s2_img_long] + 180

# df_s2_orb['LONGITUDE'] = df_s2_orb['LONGITUDE'] + 180

# Adding field in dataframe to future update
df_s2_img['NEW_NAME'] = 'NONE'
df_s2_img['SENSING_DATETIME'] = 'NONE'

for j in df_s2_img.index.values:
    df_sel_orb =  df_s2_orb.loc[df_s2_orb['ORBIT_NAME'].isin([df_s2_img.loc[j,'ORBIT_NAME']])]
    # segundo nivel de selecao - intervalo de LATITUDE
    df_sel_orb_xy = df_sel_orb.loc[((df_sel_orb['LATITUDE'] >= \
     (df_s2_img.loc[j,'LAT_IMG_MIN'])-1.)) & ((df_sel_orb['LATITUDE'] <= \
     (df_s2_img.loc[j,'LAT_IMG_MAX'])+1.)) & ((df_sel_orb['LONGITUDE'] >= \
     ((df_s2_img.loc[j,'LON_IMG_MIN'])-1.)) & (df_sel_orb['LONGITUDE'] <= \
     ((df_s2_img.loc[j,'LON_IMG_MAX'])+1.)))]
     # criar uma variavel temp para armazenar o tempo medio dos pontos
    df_s2_img.loc[j,'SENSING_DATETIME'] = df_sel_orb_xy.LOCALTIME.min() +\
     (df_sel_orb_xy.LOCALTIME.max() - df_sel_orb_xy.LOCALTIME.min())/2.

     # df_s2_img.loc[j,'SENSING_TIME'] = df_sel_orb_yy.dt.time
	
	# df_s2_img.loc[j,'LAT_ORB_MAX'] = df_sel_orb_yy.LATITUDE.max()
	# df_s2_img.loc[j,'LON_ORB_MAX'] = df_sel_orb_yy.LONGITUDE.max()
	# df_s2_img.loc[j,'LAT_ORB_MIN'] = df_sel_orb_yy.LATITUDE.min()
	# df_s2_img.loc[j,'LON_ORB_MIN'] = df_sel_orb_yy.LONGITUDE.min()
	
# # plt.scatter(df_sel_orb.LONGITUDE, df_sel_orb.LATITUDE)
# # plt.grid() 
# # plt.show() 

# df_s2_img.to_csv(wf_s2_orb + 'df_s2_img_5sec.csv', sep=',', encoding='utf-8')
print('FIM')
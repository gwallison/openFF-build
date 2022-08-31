# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

Location_cleanup updates for version 15:
    - Use shapefiles to confirm that reported lat/lon data are in the reported
      geographical entities.  Flag when they are not.
    - bgLat and bgLon will all be converted and stored in WGS84 (EPSG:4326); 
      however converting to other projections is simple when they are consistent.
    - removing some previous location flags; keeping latlon_too_coarse (but changing
      it to less than 7 decimal digits - still in the "privacy" range.)
"""
# from geopy.distance import geodesic
import pandas as pd
import numpy as np
import geopandas

import build_common
trans_dir = build_common.get_transformed_dir()

final_crs = 4326 # EPSG value for bgLat/bgLon; 4326 for WGS84: Google maps

#### -----------   File handles  -------------- ####
sources = trans_dir
cur_tab = sources+'location_curated.csv'
api_code_ref = sources+'new_state_county_ref.csv'

#### ----------    end File Handles ----------  ####

# def set_distance(row):
#     """ used to calculate the distance between two sets of lat/lon in miles.
#     If there are problems in the calculation, an arbitrarily large number is
#     returned."""
    
#     if row.refLatitude<1:
#         return 9999 # arbitrary big number; 
#     try:
#         return geodesic((row.Latitude,row.Longitude),
#                         (row.refLatitude,row.refLongitude)).miles
#     except: # problems? set to a big number
#         #print(f'geodesic exception on {row.Latitude}, {row.Longitude};; {row.refLatitude}, {row.refLongitude}')
#         return 9999 # arbitrary big number; 

def get_cur_table():
    return pd.read_csv(cur_tab,quotechar='$',encoding='utf-8')
    
def fetch_clean_loc_names(latlon_df):
    latlon_df.StateName.fillna('missing',inplace=True)
    latlon_df.CountyName.fillna('missing',inplace=True)
    old = get_cur_table()
    #print(f'old: {old.columns}')
    #print(f'latlon: {latlon_df.columns}')    
    mg = pd.merge(latlon_df,old[['StateName','StateNumber',
                                  'CountyName','CountyNumber']],
                   on=['StateName','StateNumber','CountyName','CountyNumber'],
                   how='left',indicator=True)
    #print(mg.head())
    new = mg[mg._merge=='left_only'].groupby(['StateName','StateNumber',
                                              'CountyName','CountyNumber'],
                                             as_index=False)['UploadKey'].count()
    print(f'Number of new locations: {len(new)}')
    newlen = len(new)
    if newlen>0:    
        # fetch reference
        ref = pd.read_csv(api_code_ref)
        #print(ref.head())
        
        # merge them
        mg = pd.merge(new,ref,
                      on=['StateNumber','CountyNumber'],
                      how='left')
        mg['st_ok'] = mg.StateName.str.lower()==mg.REF_StateName
        mg['ct_ok'] = mg.CountyName.str.lower()==mg.REF_CountyName
        mg['loc_name_mismatch'] = ~(mg.st_ok & mg.ct_ok)
        mg.loc_name_mismatch = np.where(mg.loc_name_mismatch,' ','False')
        mg.drop(['ct_ok','st_ok'],inplace=True,axis=1)
        mg.rename({'REF_StateName':'bgStateName',
                   'REF_CountyName':'bgCountyName'},inplace=True,axis=1)
        final = pd.concat([mg,old],sort=True)
        # SAVE IT AS A file to curate
        final[['StateName','bgStateName','CountyName','bgCountyName',
               'StateNumber','CountyNumber','loc_name_mismatch',
               'first_date','change_date','change_comment']].to_csv('./tmp/location_curated_NEW.csv',quotechar='$',
                     encoding='utf-8',index=False)
        return newlen,final
    return newlen,old # if no new

def reproject(df):
    # creates bgLat/lon that is standardized to WGS84
    print('  -- re-projecting location points')
    df['epsg'] = 4267
    df.epsg = np.where(df.Projection.str.lower()=='nad83',4269,df.epsg)
    df.epsg = np.where(df.Projection.str.lower()=='wgs84',4326,df.epsg)

    crs_types = df.epsg.unique().tolist()
    #print(f'Types of EPSG in input frame: {crs_types}')
    dfs = []
    for in_epsg in crs_types:
        t = df[df.epsg==in_epsg]
        t = geopandas.GeoDataFrame(t, geometry= geopandas.points_from_xy(t.Longitude, t.Latitude,crs=in_epsg))
        if in_epsg != final_crs:
            t.to_crs(final_crs,inplace=True)
        dfs.append(t)
    new = pd.concat(dfs)
    new['bgLatitude'] = new.geometry.y
    new['bgLongitude'] = new.geometry.x

    df.drop('epsg',axis=1,inplace=True)
    df = pd.merge(df,new[['UploadKey','bgLatitude','bgLongitude']],
                  on='UploadKey',how='left') 
    return df

def fetch_shapefiles():
    print('  -- fetching shapefiles')
    url = 'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip'
    states = geopandas.read_file(url).rename({'NAME':'StateName'},axis=1)
    states.StateName = states.StateName.str.lower()
    url = 'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_500k.zip'
    counties = geopandas.read_file(url).rename({'NAME':'CountyName'},axis=1)
    counties.CountyName = counties.CountyName.str.lower()
    # get StateName into counties
    counties = pd.merge(counties,states[['StateName','STATEFP']],
                        on='STATEFP',how='left')

    return states,counties

def get_matching_county_name(bgCounty,bgState,geolst):
    # some of the county names have spaces that the shapefile names don't
    # this routine finds a matching shapefile name
    if (bgState,bgCounty) in geolst:
        return bgCounty
    if ' ' in bgCounty:
        tCounty = bgCounty.replace(' ','')
        if (bgState,tCounty) in geolst:
            print(f'   found match for: {bgState},{bgCounty}')
            return tCounty
    print(f'   No match: {bgState}: {bgCounty}')
    return bgCounty
    

def check_against_shapefiles(locdf):
    #print(f'Number of empty bgStateName: {locdf.bgStateName.isna().sum()}')
    locdf[locdf.bgStateName.isna()].to_csv('./tmp/temp.csv')
    # first check states
    states,counties = fetch_shapefiles()
    states.to_crs(final_crs,inplace=True)
    counties.to_crs(final_crs,inplace=True)
    gb = counties.groupby(['StateName','CountyName'],as_index=False)['COUNTYFP'].first()
    glst = []
    for i,row in gb.iterrows():
        glst.append((row.StateName,row.CountyName))
    st_collect = []
    ct_collect = []
    st_ct_lst = locdf.groupby(['bgStateName','bgCountyName'],as_index=False)\
                 ['UploadKey'].count()

    print('  -- checking against shapefiles')
    for i,row in st_ct_lst.iterrows():
        #print(f'    -- {row.bgStateName} : {row.bgCountyName}')
        gCountyName = get_matching_county_name(row.bgCountyName,
                                               row.bgStateName,
                                               glst)
        state_geo = states[states.StateName==row.bgStateName]
        county_geo = counties[(counties.StateName==row.bgStateName)&\
                              (counties.CountyName==gCountyName)]

        t = locdf[(locdf.bgStateName==row.bgStateName)&\
                  (locdf.bgCountyName==row.bgCountyName)]\
            .groupby('UploadKey',as_index=False)\
            [['bgLatitude','bgLongitude']].first()
        gdf = geopandas.GeoDataFrame(t,
                                     geometry= geopandas.points_from_xy(t.bgLongitude, 
                                                                        t.bgLatitude,
                                                                        crs=final_crs))
        points_in_st = geopandas.sjoin(gdf,state_geo,how='left')  
        points_in_st['loc_within_state'] = np.where(points_in_st.STATEFP.isna(),'NO','YES')
        st_collect.append(points_in_st[['UploadKey','loc_within_state']])
        points_in_ct = geopandas.sjoin(gdf,county_geo,how='left')  
        points_in_ct['loc_within_county'] = np.where(points_in_ct.STATEFP.isna(),'NO','YES')
        ct_collect.append(points_in_ct[['UploadKey','loc_within_county']])

    state_flag = pd.concat(st_collect,sort=True)
    final = pd.merge(locdf,state_flag,on='UploadKey',how='left')
    county_flag = pd.concat(ct_collect,sort=True)
    final = pd.merge(final,county_flag,on='UploadKey',how='left')

    final.loc_within_state.fillna('unknown',inplace=True)        
    final.loc_within_county.fillna('unknown',inplace=True)        
    return final
        
def save_upload_ref(df,data_source):
    """save the data frame that serves as an uploadKey reference; in particular
    best guesses on location data """
    upload_ref_fn = sources+f'{data_source}/uploadKey_ref.csv'

    df[['UploadKey','StateName','bgStateName','CountyName','bgCountyName',
        'Latitude','bgLatitude','Longitude','bgLongitude',
        'latlon_too_coarse','loc_name_mismatch',
        'loc_within_state','loc_within_county']]\
            .to_csv(upload_ref_fn,quotechar='$',
                                     encoding='utf-8',
                                     index=False)


def get_decimal_len(s):
    """used to find the length of the decimal part of  lan/lon"""
    t = str(s)
    for c in t:
        if c not in '-.0123456789':
            return -1
    if '.' not in t:
        return 0
    while '.' in t:
        try:
            t = t[1:]
        except:
            pass
    return len(t)

def get_latlon_df(rawdf):
    #print(rawdf.columns)
    return rawdf.groupby('UploadKey',as_index=False)\
                                [['Latitude','Longitude',
                                  'Projection',
                                  'StateNumber','CountyNumber',
                                  'StateName','CountyName']].first()
    
def find_latlon_problems(locdf):
    print('Make list of disclosures whose lat/lons are not specific enough')
    locdf['latdeclen'] = locdf.Latitude.map(lambda x: get_decimal_len(x))
    locdf['londeclen'] = locdf.Longitude.map(lambda x: get_decimal_len(x))
    locdf['latlon_too_coarse'] = (locdf.londeclen+locdf.latdeclen)<7
    # flag empty  or obviously wrong lat/lon     
    return locdf.drop(['latdeclen','londeclen'],axis=1)



##########  Main script ###########
def clean_location(rawdf,data_source='bulk'):
    print('Starting Location cleanup')
    locdf = get_latlon_df(rawdf)
    rawlen = len(locdf)
    assert locdf.UploadKey.duplicated().sum()==0
    locdf = find_latlon_problems(locdf)
    assert len(locdf)== rawlen
    
    newlen,clean_names = fetch_clean_loc_names(locdf)

    reproj_df = reproject(locdf)
    assert reproj_df.UploadKey.duplicated().sum()==0
    assert len(reproj_df)== rawlen

    # merge them
    locdf = pd.merge(reproj_df,
                      clean_names[['StateName','CountyName',
                                   'StateNumber','CountyNumber',
                                   'bgStateName','bgCountyName','loc_name_mismatch']],
                      on=['StateName','CountyName','StateNumber','CountyNumber'],
                      how='left')
    assert locdf.UploadKey.duplicated().sum()==0
    assert len(locdf)== rawlen
    final = check_against_shapefiles(locdf)
    assert final.UploadKey.duplicated().sum()==0
    assert len(final)== rawlen

    save_upload_ref(final,data_source)
    return newlen
    
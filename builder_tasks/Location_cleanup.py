# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison


Change the file handles at the top of this code to appropriate directories.
Note that this version processes everything from scratch after  I saw how
silent changes can be in lat/lon variables.

This script is used to analyze and adjust the location data of FracFocus. It results
in 4 gnerated fields (bgLatitude, bgLongitude, bgStateName, and bgCountyName) as
well as a set of flags that will go into the record_flags:
    L - general indication of a location issue or problem, set for all below
    F - Lat/lon are flipped.  bgLatitude=Longitude and vice versa
    G - lat/lon are probably insuffieciently detailed to work in well pad
          identification.  bglat/lon are the same as raw
    O - lat or lon are out of physical range - bglat/lon set to county centers
    D - lat or lon appear to be out of the recorded county.  bglat/lon are the same as raw
---
    N - State/county names don't match reference. bgStateName/County corrected 
        to proper names
    
"""
from geopy.distance import geodesic
import pandas as pd
import numpy as np
#from core.Geoclusters import Geoclusters

import common
trans_dir = common.get_transformed_dir()

#### -----------   File handles  -------------- ####
sources = trans_dir
upload_ref_fn = sources+'uploadKey_ref.csv'

#### ----------    end File Handles ----------  ####

def set_distance(row):
    """ used to calculate the distance between two sets of lat/lon in miles.
    If there are problems in the calculation, an arbitrarily large number is
    returned."""
    
    if row.refLatitude<1:
        return 9999 # arbitrary big number; 
    try:
        return geodesic((row.Latitude,row.Longitude),
                        (row.refLatitude,row.refLongitude)).miles
    except: # problems? set to a big number
        print(f'geodesic exception on {row.Latitude}, {row.Longitude};; {row.refLatitude}, {row.refLongitude}')
        return 9999


def save_upload_ref(df):
    """save the data frame that serves as an uploadKey reference; in particular
    best guesses on location data as well as the date that the event was
    added to this project (as opposed to the date it first appears in FF)"""
    
    df[['UploadKey','StateName','bgStateName','CountyName','bgCountyName',
        'Latitude','bgLatitude','Longitude','bgLongitude',#'clusterID',
        'latlon_too_coarse',#'geo_distance',
        'loc_name_error','latlon_out_of_range',
        'flipped_loc']].to_csv(upload_ref_fn,quotechar='$',encoding='utf-8',
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
    locdf['latlon_too_coarse'] = (locdf.londeclen+locdf.latdeclen)<5
    # flag empty  or obviously wrong lat/lon 
    
    locdf['in_range_lat'] = (locdf.Latitude>5)&(locdf.Latitude<90)
    locdf['in_range_lon'] = (locdf.Longitude<-5)&(locdf.Longitude>-180)
    
    return locdf

def latlon_adjustments(t):
    # determine distance of point from the county's reference point
    t['geo_distance'] = t.apply(lambda x: set_distance(x),axis=1)
    
    
    # consider county as a circle: what is radius?
    t['county_radius'] = (t['refTotalAreaMi^2']/3.14)**0.5                            
    t['bgLatitude'] = np.where(t.in_range_lat,t.Latitude,t.refLatitude)
    t['bgLongitude'] = np.where(t.in_range_lon,t.Longitude,t.refLongitude) 
    t['latlon_out_of_range'] = ~(t.in_range_lat&t.in_range_lon)

    # detect potentially swapped lat lon.
    t['flipped_loc'] = (t.Longitude>5)&(t.Longitude<90)&(t.Latitude<-5)&(t.Latitude>-180)                                
    t.bgLatitude = np.where(t.flipped_loc,t.bgLongitude,t.bgLatitude)
    t.bgLongitude = np.where(t.flipped_loc,t.bgLatitude,t.bgLongitude)
    
    # set the rest of bglat/bglon to lat/lon (most) 
    t.bgLatitude = np.where(t.bgLatitude.isna(),t.Latitude,t.bgLatitude)
    t.bgLongitude = np.where(t.bgLongitude.isna(),t.Longitude,t.bgLongitude)

    return t

def check_names(t):
    t['lcStateName'] = t.StateName.str.strip().str.lower()
    t['lcCountyName'] = t.CountyName.str.strip().str.lower()
    
    t['loc_name_error'] = (t.lcStateName!=t.refStateName)|(t.lcCountyName!=t.refCountyName)
    t['bgStateName'] = t.refStateName # always use the ref name
    t['bgCountyName'] = t.refCountyName
    return t    


##########  Main script ###########
def clean_location(rawdf):
    print('Starting Location cleanup')
    locdf = get_latlon_df(rawdf)
    locdf = find_latlon_problems(locdf)

    #### Get reference data
    
    ref = pd.read_csv(trans_dir+'state_county_ref.csv')
    ref = ref[['refStateName','refCountyName','refStateNumber','refCountyNumber',
               'refLatitude','refLongitude','refTotalAreaMi^2']]
    
    # first compare lat/lon by state and coundy CODES
    t = pd.merge(locdf,ref,left_on=['StateNumber','CountyNumber'],
                 right_on=['refStateNumber','refCountyNumber'],how='left')
    print('  -- lat lon adjustments')
    t = latlon_adjustments(t)
    print('  -- check names')
    t = check_names(t)

    save_upload_ref(t)
# =============================================================================
#     ## now make clusters
#     
#     gc = Geoclusters(locdf=t) #[:100])
#     ev = gc.make_simple_clusters()
#     
#     save_upload_ref(ev)
# 
# =============================================================================



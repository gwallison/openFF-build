# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 11:21:28 2022

@author: Gary

This script is used to transform the scraped data from the FFVersion 1 PDFs
into a format compatible with the bulk download data.  This is primarily used
by Bulk_data_reader.py


The input data for this process is in two files and much of the helper
fields for the scraping process are still in the files.  
"""

import pandas as pd
import numpy as np
import os
import re

chemfn = 'fresh_chem_concat.csv'
metafn = 'fresh_meta_with_bulk.csv'
filterfn = 'col_shift_detect_final.csv'

def remove_cid_junk(s):
    # the artefact "(cid:dd)" is found in many disclosures.  Remove it
    try:
        return re.sub(r'\(cid\:\d*\)','',s) # get rid of those (cid:num) artefacts
    except:
        return s
    
def is_float(s):
    try:
        _ = float(s)
        return True
    except:
        return False
    
def xform_string(s):
    # For most cells, this is a conversion of string to float. 
    #a handful of values give a range instead of a single value.  As indicated
    #  by FF, the larger value should be used.
    # when this cell contains anything else, including "proprietary" etc., it
    # returns np.NaN.
    try:
        s = remove_cid_junk(s)
        return float(s)        
    except:
        try:
            s = remove_cid_junk(s)
            lst = s.split('-')
            x = float(lst[1])
            return x
        except:
            pass
    return np.NAN

def xform_meta(s):
    try:
        return float(s)
    except:
        try:
            s = s.replace('(',' (') # makes space before the "(cid)" junk
            s = remove_cid_junk(s)
            s = s.split()[0]  #take just first chunk before space
            s = s.replace(',','') # remove commas 
            s = s.replace("'",'') # remove "foot" tick mark (for TVD) 
            s = s.replace('Gnrl','') # weird artefact in some disclosures (in PDF)
            out = float(s)
            return out
        except:
            return np.NaN
        
        

def get_FFV1(sources = r"e:\OpenFF_mirror\data"):
    # TradeName, Supplier and Purpose are extended to all components of
    # a product listed in a disclouse.  This is done in the pre-processing.
    # Here we exchange the extended versions for the PDF-literal versions. This
    # makes the PDF versions compatible with the bulk download.
    #
    # Also we filter out all those disclosures that were detected as
    # poorly formatted.
    
    filterdf = pd.read_csv(os.path.join(sources,'bulk_data/',filterfn),
                        quotechar='$',encoding='utf-8',low_memory=False)
    cond = filterdf.in_final
    keepupk = filterdf[cond].UploadKey.unique().tolist()

    chemdf = pd.read_csv(os.path.join(sources,'bulk_data/',chemfn),
                      quotechar='$',encoding='utf-8',low_memory=False,
                      dtype={'PercentHFJob':'str',
                             'PercentHighAdditive':'str',
                             'row_num':'int'})
    chemdf = chemdf[chemdf.UploadKey.isin(keepupk)]
    #print('   -- cleaning "(cid...)"')
    for col in ['TradeName','Supplier','Purpose','IngredientName',
                'CASNumber','PercentHFJob','PercentHighAdditive',
                'Comment']:
        chemdf[col] = chemdf[col].map(lambda x: remove_cid_junk(x))
    print(f'Number of scraped disclosures in filtered set: {len(chemdf.UploadKey.unique())}')

    #transform the string versions of PercentHFJob into something compatible
    # with the standard bulk download.
    
    #!!!!!!!!!!!!!
    # For now, put modified PercentHFJob and additive back into PercentHFJob
    # chemdf['bgPercentHFJob'] = chemdf.PercentHFJob.map(lambda x: xform_string(x))    
    # chemdf['bgPercentHighAdditive'] = chemdf.PercentHighAdditive.map(lambda x: xform_string(x))    
    chemdf['PercentHFJob'] = chemdf.PercentHFJob.map(lambda x: xform_string(x))    
    chemdf['PercentHighAdditive'] = chemdf.PercentHighAdditive.map(lambda x: xform_string(x))    
    chemdf.TradeName = chemdf.exTN
    chemdf.Purpose = chemdf.exPur
    chemdf.Supplier = chemdf.exSup
    chemdf['IngredientKey'] = chemdf.UploadKey + '-ROW:' +chemdf.row_num.astype('str').str.zfill(4)
    #print(chemdf.columns)
    cond = ~(chemdf.row_type=='empty')
    chemdf = chemdf[cond][['row_type', 'UploadKey',
                     'TradeName', 'Supplier', 'Purpose', 
                     'IngredientName', 'CASNumber', 
                     'PercentHighAdditive', 'PercentHFJob',
                     'Comment', 
                     # !!!!!!!!!!!!!!!!!!!!!!!
                     #'bgPercentHFJob', 'bgPercentHighAdditive', 
                     'IngredientKey']]
    #print(f'Len of chemdf: {len(chemdf)}, disc: {len(chemdf.UploadKey.unique())}')

    #now the meta file
    metadf = pd.read_csv(os.path.join(sources,'bulk_data/',metafn),
                      quotechar='$',encoding='utf-8',low_memory=False,
                      dtype={'APINumber':'str'})
    
    # metadf.APINumber = metadf.bulkAPINumber
    # metadf.date = metadf.bulkDate
    # metadf['StateNumber'] = metadf.APINumber.str[:2].astype('int')
    # metadf['CountyNumber'] = metadf.APINumber.str[2:5].astype('int')
    
    metadf.TotalBaseWaterVolume = metadf.TotalBaseWaterVolume.map(lambda x: xform_meta(x))
    metadf.TVD = metadf.TVD.map(lambda x: xform_meta(x))
    # metadf.Longitude = metadf.Longitude.map(lambda x: xform_meta(x))
    # metadf.Latitude = metadf.Latitude.map(lambda x: xform_meta(x))
    #metadf.APINumber = metadf.APINumber.str.replace('Gnrl','') # weird artefact

    mg = pd.merge(chemdf,metadf,on='UploadKey',how='left')
    # print(f'Num only: chem: {len(mg[mg._merge=="left_only"])} meta: {len(mg[mg._merge=="right_only"])}')
    # print(f'Num only: both: {len(mg[mg._merge=="both"])} ')
    mg.UploadKey = 'scrape-'+mg.UploadKey.str[:]
    mg['data_source'] = 'FFV1_scrape'
    #print(mg.iloc[1000].T)
    #print(len(mg[mg.APINumber.str[:2]=='05'].UploadKey.unique()))
    
    # add empty fields to be compatible with bulk data
    mg['IndianWell'] = 'unknown'
    mg['FederalWell'] = 'unknown'
    mg['TotalBaseNonWaterVolume'] = 'unknown'
    mg['FFVersion'] = 'unknown'
    mg['MassIngredient'] = np.NaN
    mg['ingKeyPresent'] = True
    mg['IngredientComment'] = ''
    mg['density_from_comment'] = np.NaN
    return mg
    
if __name__ == '__main__':
    t = get_FFV1()
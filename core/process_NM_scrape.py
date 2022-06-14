# -*- coding: utf-8 -*-
"""
Created on Thu Jun  2 14:58:05 2022

@author: Gary

This contains the code to assemble a data set from the html files
downloaded from the New Mexico state web site
"""

import pandas as pd
import numpy as np
import os

finalfn = r"C:\MyDocs\OpenFF\data\bulk_data\NM_scrape_2022_05.csv"

def get_suffix(link):
    loc = link.find('Id=')
    return link[loc+3:]

def get_id(txt):
    return txt.split('\n')[0]

def get_var_by_loc(tdf,tnum,row=0,col=0):
    return tdf[tnum].iloc[row][col]
 

def get_file_map():
    froot = r"E:\working\NM\html_download"
    #froot = r"F:\NM_scrape_5_2022\html_download"
    fn_tmp1 = r"wwwapps.emnrd.nm.gov\OCD\OCDPermitting\Report\Fracking\FrackingFluidDisclosure_PermitID_"
    fn_tmp2 = ".aspx.html"
    rooturl = "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Report/Fracking/FrackingFluidDisclosure.aspx?PermitID="

    t = pd.read_csv(r"C:\MyDocs\OpenFF\src\PSR_projects\NM\links_to_html.csv")
    t['suffix'] = t.links.map(lambda x: get_suffix(x))
    t['suffix'] = t.suffix.str.replace(',','_')
    t['html_link'] = rooturl+t.suffix
    t['NM_id'] = t.text.map(lambda x: get_id(x)) 
    fns = []
    for i,row in t.iterrows(): 
        fns.append(os.path.join(froot,row.NM_id,fn_tmp1+row.suffix+fn_tmp2))
    t['file_link'] = fns
    return t
                   
#  fetch data from a single disclosure (tdf)
def get_disclosure_df(tdf,NM_id,prefix='NM_scrape_2022_05_'):
    # first the meta values
    lat = get_var_by_loc(tdf,3,0,0).split()[2]
    lon = get_var_by_loc(tdf,3,0,1).split()[1]
    API = get_var_by_loc(tdf,0,1,2).split()[4].replace('-','')+'0000'
    Well = " ".join(get_var_by_loc(tdf,0,2,2).split()[3:])
#     Well = get_var_by_loc(tdf,0,2,2)
    op = " ".join(get_var_by_loc(tdf,4,0,0).split()[5:])
    date = get_var_by_loc(tdf,4,2,1)
    fc = get_var_by_loc(tdf,4,2,3)
    tvd = get_var_by_loc(tdf,4,4,0).split()[5].replace(',','')
    tbwv = get_var_by_loc(tdf,4,4,1).split()[6].replace(',','')
    ru_vol = get_var_by_loc(tdf,4,5,0).split()[7].replace(',','')
    county = get_var_by_loc(tdf,0,14,0).split()
    county = " ".join(county[2:])
    
    # now chem
    chem = tdf[6].copy()
    chem = chem[:-1]
    #tn = []; sup = []; purp = []; ing = []; cas = []; perHiAdd = []; perHFJ = []
    chem.columns = ['TradeName','Supplier','Purpose','IngredientName','CASNumber','PercentHighAdditive','PercentHFJob']
    chem.PercentHighAdditive = chem.PercentHighAdditive.str.replace('%','').astype('float')
    chem.PercentHFJob = chem.PercentHFJob.str.replace('%','').astype('float')
    chem['UploadKey'] = prefix+NM_id
    chem['CountyName'] = county
    chem['Latitude'] = lat
    chem['Longitude']= lon
    chem['APINumber'] = API
    chem['StateNumber'] = API[:2]
    chem['CountyNumber'] = API[2:5]
    chem['WellName'] = Well
    chem['OperatorName'] = op
    chem['JobEndDate'] = date
    chem['FracCompany'] = fc
    chem['TVD'] = tvd
    chem['TotalBaseWaterVolume'] = tbwv
    chem['Reuse_volume'] = ru_vol
    chem['StateName'] = 'New Mexico'
    chem['Projection']  = 'NAD27'  # this is the most common projectin in FF New Mexico   
    return chem


def make_dataset():
    fdf = get_file_map()
    print(f'Starting NM_scrape processing. Total disclosures: {len(fdf)}')
    dfs = []
    for i,row in fdf.iterrows():
        if (i%100==0):
            print(f'Finished {i} of {len(fdf)}')
        #print(fdf.iloc[i].file_link)
        tdf = pd.read_html(fdf.iloc[i].file_link)
        dfs.append(get_disclosure_df(tdf,NM_id=row.NM_id))
    out  = pd.concat(dfs,sort=True)
    out['IndianWell'] = 'unknown'
    out['FederalWell'] = 'unknown'
    out['TotalBaseNonWaterVolume'] = 'unknown'
    out['FFVersion'] = 'unknown'
    out['MassIngredient'] = np.NaN
    out['ingKeyPresent'] = True
    out['JobStartDate'] = out.JobEndDate
    out['data_source'] = 'NM_scrape_2022_05'
    out['IngredientComment'] = ''
    out['density_from_comment'] = np.NaN
    out['str_idx'] =  out.index.astype(int).astype('str')
    out['IngredientKey'] = out.UploadKey.str[:]+'::'+ out.str_idx
    out.drop('str_idx',axis=1,inplace=True)

    out.to_csv(finalfn,index=False)
    
def fetch_final():
    return pd.read_csv(finalfn,
                       dtype={'APINumber':'str'})


if __name__ == '__main__':     
    make_dataset()

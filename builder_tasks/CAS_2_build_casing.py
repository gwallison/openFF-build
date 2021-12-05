# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 13:09:22 2021

@author: Gary
"""

import pandas as pd
import numpy as np

import build_common
trans_dir = build_common.get_transformed_dir()


def make_casing(df): 
    #print(df.info()) 
    casing = df.groupby(['CASNumber','IngredientName'],as_index=False)['UploadKey'].count()
    casing.columns = ['CASNumber','IngredientName','num_rec']
    #print(casing.head())
    master = pd.read_csv(trans_dir+'casing_curated.csv',quotechar='$',encoding='utf-8')
    old = master[['CASNumber','IngredientName']]
    casing = pd.merge(casing,old,on=['CASNumber','IngredientName'],
                   how='outer',indicator=True)
    casing = casing[casing['_merge']=='left_only']
    
    print(f'Number of new CAS|ING pairs: {len(casing)}')
    if len(casing)==0:
        return master
    CAS_cur = pd.read_csv(trans_dir+'CAS_curated.csv',quotechar='$',
                          encoding='utf-8')
    ING_cur = pd.read_csv(trans_dir+'ING_curated.csv',quotechar='$',
                          encoding='utf-8')
    ING_cur[ING_cur.IngredientName.duplicated(keep=False)].to_csv('./tmp/ING_cur_dups.csv',
                                                                  quotechar='$')
    mg = pd.merge(casing,CAS_cur,on='CASNumber',how='left',validate='m:1')
    mg = pd.merge(mg,ING_cur,on='IngredientName',how='left',validate='m:1')
    #mg = mg.rename({'bgSource':'bgSource_old'},axis=1)
    #print(mg.columns)
    c_ing_numeric = mg.prospect_CAS_fromIng.str[0].isin(['0','1','2','3','4','5','6','7','8','9'])
    c_cas_numeric = mg.curatedCAS.str[0].isin(['0','1','2','3','4','5','6','7','8','9'])

    mg['bgSource'] = np.where(c_ing_numeric&c_cas_numeric&(mg.curatedCAS==mg.prospect_CAS_fromIng),
                                 'both','UNK')
    mg.bgSource = np.where((~c_ing_numeric)&c_cas_numeric,'CAS_only',
                           mg.bgSource)
    mg.bgSource = np.where(c_ing_numeric&(~c_cas_numeric),'ING_only',
                           mg.bgSource)
    mg.bgSource = np.where((~c_ing_numeric)&(~c_cas_numeric),'neither',
                           mg.bgSource)
    mg.bgSource = np.where(c_ing_numeric&c_cas_numeric&(mg.curatedCAS!=mg.prospect_CAS_fromIng),
                                 'conflict',mg.bgSource)
    
    mg['bgCAS'] = 'unassigned'
    mg.bgCAS = np.where(mg.bgSource=='both',mg.curatedCAS,mg.bgCAS)
    mg.bgCAS = np.where(mg.bgSource=='CAS_only',mg.curatedCAS,mg.bgCAS)
    mg.bgCAS = np.where(mg.bgSource=='ING_only',mg.prospect_CAS_fromIng,mg.bgCAS)
    mg.bgCAS = np.where(mg.bgSource=='neither','ambiguousID',mg.bgCAS)
    mg.bgCAS = np.where(mg.bgSource=='conflict','conflictingID',mg.bgCAS)
    mg.bgCAS = np.where(mg.categoryCAS=='proprietary','proprietary',mg.bgCAS)
    mg.bgCAS = np.where((mg.bgSource=='neither')&(mg.prospect_CAS_fromIng=='proprietary'),
                        'proprietary',mg.bgCAS)

    
    mg = mg[~mg.duplicated()]
    print(mg.value_counts('bgSource'))
    mg = pd.concat([mg,master],sort=True)
    mg[['CASNumber','IngredientName','categoryCAS','syn_code','bgSource',
        'prospectCAS','curatedCAS','recog_syn',
        'prospect_CAS_fromIng','alt_CAS','bgCAS',
        'first_date','change_date','change_comment']].to_csv('./tmp/casing_NEW.csv',
                                                quotechar='$',encoding='utf-8',
                                                index=False)
    # mg.to_csv('./tmp/casing_NEW.csv',quotechar='$',encoding='utf-8',index=False)
    return mg
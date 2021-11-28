# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

This creates the initial data frame of the CAS master list.  

The CASNumber and IngredientName values are unmodified. They may contain
new line characters, etc.
"""
#import numpy as np
import pandas as pd
import numpy as np
import sys
import core.cas_tools as ct
import common

trans_dir = common.get_transformed_dir()
        
def initial_CAS_master_list(rawdf): # rawdf
    old = pd.read_csv(trans_dir+'CAS_curated.csv',quotechar='$',
                            encoding='utf-8')
    old = old[['CASNumber']]
    ct.na_check(old,txt='CAS_1 for old')
    new = rawdf.groupby(['CASNumber'],as_index=False).size()

    print('checking for non printables in CASNumber...')
    new.CASNumber.map(lambda x: ct.has_non_printable(x))
    #print('checking for non printables in IngredientName...')

    mg = pd.merge(new,old,on=['CASNumber'],
                  how = 'outer',indicator=True)
    ct.na_check(old,txt='CAS_1 for mg')
    new = mg[mg['_merge']=='left_only'].copy() # only want new stuff
    new = new[['CASNumber']]    
    new['clean_wo_work'] = new.CASNumber.map(lambda x: ct.is_valid_CAS_code(x))
    
    new['tent_CAS'] = new.CASNumber.map(lambda x:ct.cleanup_cas(x))
    new['valid_after_cleaning'] = new.tent_CAS.map(lambda x: ct.is_valid_CAS_code(x))
    ct.na_check(new,txt='CAS_1 for new')   
    new['auto_status'] = np.where(new.clean_wo_work,'perfect','unk')
    new.auto_status = np.where((new.auto_status=='unk')&(new.valid_after_cleaning),
                               'cleaned',new.auto_status)
    return new

def merge_CAS_with_ref(df):
    # df is the new casigs with cas_tool fields included
    # fetch the reference dataframes
    ref = pd.read_csv(trans_dir+'CAS_ref_and_names.csv',
                      encoding='utf-8',quotechar='$')
    dep = pd.read_csv(trans_dir+'CAS_deprecated.csv',encoding='utf-8',quotechar='$')
    
    # get the matches with reference numbers
    test = pd.merge(df, #[['CASNumber','tent_CAS','valid_after_cleaning']],
                    ref[['cas_number']],
                    left_on='tent_CAS',right_on='cas_number',how='left',
                    indicator=True)
    test['on_ref_list'] = np.where(test['_merge']=='both',
                                   'verified;normal','unk') 
    test['CAS_prospect'] = np.where(test['_merge']=='both',
                             test.cas_number, # if in both, save the CAS
                             '') # otherwise leave it empty
    test = test.drop('_merge',axis=1) # clean up before next merge

    # now find the deprecated CAS numbers
    test = pd.merge(test,dep,
                    left_on='tent_CAS',right_on='deprecated',how='left',
                    indicator=True)
    # A potential error is if we get an authoritative match AND a deprecated
    #   match.  Scan for that situation, alert the user, and exit
    cond1 = ~test.cas_number.isna()
    cond2 = test['_merge']=='both'
    if (cond1&cond2).sum()>0:
        print('DEPRECATED DETECTED ON AN VERIFIED CAS')
        print(test[cond1&cond2])
        sys.exit(1)
        
    # mark the deprecated and take the valid CAS as bgCAS
    test['on_ref_list'] = np.where(test['_merge']=='both',
                              'verified;from deprecated',test.on_ref_list) 
    test['CAS_prospect'] = np.where(test['_merge']=='both',
                             test.cas_replacement,test.CAS_prospect)
    test = test.drop(['_merge','cas_number'],axis=1) # clean up before next merge
    
    # mark the CAS numbers that are formally valid but without authoritative cas in ref.
    #  these may be good targets for later curating
    cond1 = test.valid_after_cleaning
    cond2 = test.on_ref_list=='unk'
    test['CAS_prospect'] = np.where(cond1&cond2,'valid_but_empty',test.CAS_prospect)
    test['on_ref_list'] = np.where(cond1&cond2,'valid_but_empty',test.on_ref_list)
    test = test.drop(['deprecated',
                      'cas_replacement','tent_CAS',
                      #'ing_name',
                      'valid_after_cleaning'],axis=1) # clean up before next merge
    test['is_new'] = True
    # Now concat with the old data (DONT MERGE - otherwise old gets clobbered!)
    print(f'\nNumber of new CAS lines to curate: {len(test)}\n')
    old = pd.read_csv(trans_dir+'CAS_curated.csv',quotechar='$',
                            encoding='utf-8')
    # old = old[['CASNumber','bgCAS','category',
               # 'close_syn','comment','first_date','change_date','change_comment']]
    old['is_new'] = False    
    out = pd.concat([test,old],sort=True)
    
    # return out, len(test)   
    return out[['CASNumber','prospectCAS','auto_status','on_ref_list',
                'curatedCAS','categoryCAS','is_new',
                'comment','first_date',
                'change_date','change_comment']],len(test)


def initial_Ing_master_list(rawdf): # rawdf
    old = pd.read_csv(trans_dir+'ING_curated.csv',quotechar='$',
                            encoding='utf-8')
    #print(old[0:1])
    test = old[['IngredientName']]
    ct.na_check(test,txt='CAS_1 for old')
    new = rawdf.groupby(['IngredientName'],as_index=False).size()

    print('checking for non printables in IngredientName...')
    new.IngredientName.map(lambda x: ct.has_non_printable(x))

    mg = pd.merge(new,test,on=['IngredientName'],
                  how = 'outer',indicator=True)
    ct.na_check(test,txt='ING_1 for mg')
    new = mg[mg['_merge']=='left_only'].copy() # only want new stuff
    new = new[['IngredientName']]    
    ct.na_check(new,txt='ING_1 for new')
    new.to_csv('./tmp/ING_to_curate.csv',quotechar='$',encoding='utf-8')
    return new

# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 07:32:13 2021

@author: Gary

In this script, the cas master list is merged with the CAS reference list
created from the SciFinder searches.  

The steps in this process:
- fetch the reference dataframes for authoritative CAS numbers and deprecated ones.
- find and mark all tentative_CAS numbers that match authoritative numbers
- find and mark all that match deprecated numbers
- find and mark those 'valid-but-empty' cas numbers, mark them.

"""
import numpy as np
import pandas as pd
import sys

       
def merge_with_ref(df):
    # df is the new casigs with cas_tool fields included
    # fetch the reference dataframes
    ref = pd.read_csv('./sources/CAS_ref_and_names.csv',
                      encoding='utf-8',quotechar='$')
    dep = pd.read_csv('./sources/CAS_deprecated.csv',encoding='utf-8',quotechar='$')
    
    # get the matches with reference numbers
    test = pd.merge(df, #[['CASNumber','tent_CAS','valid_after_cleaning']],
                    ref[['cas_number']],
                    left_on='tent_CAS',right_on='cas_number',how='left',
                    indicator=True)
    test['on_ref_list'] = np.where(test['_merge']=='both',
                                   'verified;normal','unk') 
    test['bgCAS'] = np.where(test['_merge']=='both',
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
    test['bgCAS'] = np.where(test['_merge']=='both',
                             test.cas_replacement,test.bgCAS)
    test = test.drop(['_merge','cas_number'],axis=1) # clean up before next merge
    
    # mark the CAS numbers that are formally valid but without authoritative cas in ref.
    #  these may be good targets for later curating
    cond1 = test.valid_after_cleaning
    cond2 = test.on_ref_list=='unk'
    test['bgCAS'] = np.where(cond1&cond2,'valid_but_empty',test.bgCAS)
    test['on_ref_list'] = np.where(cond1&cond2,'valid_but_empty',test.on_ref_list)
    test = test.drop(['deprecated',
                      'cas_replacement','tent_CAS',
                      #'ing_name',
                      'valid_after_cleaning'],axis=1) # clean up before next merge
    test['is_new'] = True
    # Now concat with the old data (DONT MERGE - otherwise old gets clobbered!)
    print(f'\nNumber of new CAS/Ing lines to curate: {len(test)}\n')
    old = pd.read_csv('./sources/casing_curate_master.csv',quotechar='$',
                            encoding='utf-8')
    old = old[['CASNumber','IngredientName','bgCAS','category',
               'close_syn','comment','first_date','change_date','change_comment']]
    old['is_new'] = False    
    out = pd.concat([test,old],sort=True)
    
    return out[['CASNumber','IngredientName','bgCAS','category','is_new',
                'clean_wo_work','on_ref_list',
               'close_syn','comment','first_date',
               'change_date','change_comment']],len(test)


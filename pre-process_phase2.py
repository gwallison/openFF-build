# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

In this phase, we combine the curated CASNumber and IngredientName files
into a merged "casing" file.
First, we scan the input data for all unique pairs of CAS/IngName.

Then we connect the curated CAS and IngName files to these pairs.

From there, we decide if those two sources of information are consistent
and we assign a 'best guess' CAS number to the pair.
"""
import pandas as pd
import builder_tasks.CAS_1_make_master_list as CAS1
import builder_tasks.IngName_curator as IngNc
import common
trans_dir = common.get_transformed_dir()


# fetch the raw df pickle made in pre-process_1
rawdf = pd.read_pickle('./tmp/carrier_df.pkl')[['CASNumber','IngredientName',
                                             'UploadKey']]
# Make list of Ingredient names not yet curated
ING_to_curate = CAS1.initial_Ing_master_list(rawdf)

ING_curated = pd.read_csv(trans_dir+'ING_curated.csv',quotechar='$',encoding='utf-8')

s = """\n\n
The second stage of pre-processing has been completed.

NEXT STEPS:
        
        The list of IngredientNames curated has been updated with the
        IngName_curator tool to classify
        the new Ingredient names.  Examine the file
        /tmp/ING_curatedNEW.csv and
        replace the /sources version.  In particular, you are looking
        to curate proprietary claims.

        
"""

if len(ING_to_curate) > 0:
    print(f'Number of Names to curate: {len(ING_to_curate)}')
    refdic = IngNc.build_refdic()
    refdic = IngNc.summarize_refs(refdic)
    fullscan_df = IngNc.full_scan(ING_to_curate,refdic,pd.DataFrame())
    IngNc.analyze_fullscan(fullscan_df, ING_curated)
    # pass

    print(s)
else:
    print('\nNo new IngredientName values to process')
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
# import builder_tasks.CAS_1_make_master_list as CAS1
# import builder_tasks.IngName_curator as IngNc

import builder_tasks.CAS_2_build_casing as bc

# fetch the raw df pickle made in pre-process_1
rawdf = pd.read_pickle('./tmp/carrier_df.pkl')[['CASNumber','IngredientName',
                                             'UploadKey']]

_ = bc.make_casing(rawdf)

s = f"""\n
The third stage of pre-processing has been completed.

NEXT STEPS:
        
        The list of new pairs has been generated and intially curated.
        Examine the file  /tmp/casing_curatedNEW.csv and
        replace the /data/transformed version.  
        
        Targets of curation should be:
            - conflicting bgSources
            - 

        
"""
print(s)
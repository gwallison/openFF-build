# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

"""
#import numpy as np
import gc
import builder_tasks.CAS_1_make_master_list as CAS1
import builder_tasks.CompanyNames_make_list as complist
import builder_tasks.Location_cleanup as loc_clean

import core.Data_set_constructor as set_const
#import core.Bulk_data_reader as rff


bulk_fn = 'currentData'
stfilename = 'sky_truth_final'
make_output_files = False
do_abbrev = False
data_source = 'SkyTruth'
data_source = 'bulk'

startfile = 0 # 0 for full set
endfile = None  # None for no upper limit

master_raw = set_const.Data_set_constructor(bulk_fn=bulk_fn, const_mode='pre-process',
                                   stfilename=stfilename,         
                                   make_files=make_output_files,
                                   startfile=startfile,
                                   endfile=endfile,
                                   abbreviated=do_abbrev,
                                   data_source=data_source)\
             .get_full_raw_df()



# save part of raw_df for the carrier curation process
master_raw[['UploadKey','CASNumber','IngredientName',
            'PercentHFJob','Purpose','IngredientKey',
            'TotalBaseWaterVolume','MassIngredient',
            'TradeName']].to_pickle('./tmp/carrier_df.pkl')


rawdf = master_raw[['CASNumber','IngredientName',
                    'OperatorName','Supplier',]].fillna('MISSING')


cas1 = CAS1.initial_CAS_master_list(rawdf)
df, cas_to_curate = CAS1.merge_CAS_with_ref(cas1)
df.to_csv('./tmp/CAS_to_curateNEW.csv',index=False,encoding='utf-8',quotechar='$')

c_xlate_to_curate = complist.add_new_to_Xlate(rawdf)

newloc = loc_clean.clean_location(master_raw,data_source=data_source)



s = f"""\n\n
The first stage of pre-processing has been completed.

Number of new locations to curate: {newloc}
Number of new CAS to curate:       {cas_to_curate}
Number of new Companies to curate: {c_xlate_to_curate}

NEXT STEPS:
    
     1) If there are new State-County/Name-Number combinations, the location
        list must be curated to recognize them.  /tmp/location_curatedNEW.csv.
        After you do that, run this script again.
        
     2) The CAS_to_curate list has now been updated with new disclosures and
        is ready for the CURATION step.  Find it as /tmp/CAS_to_curateNEW.csv.

        *****
        If there are new CAS numbers that are valid but unknown, run them through
        the SciFinder process before proceeding to the curating process. Export
        whatever SciFinder gives you to a tagged output and move to the 
        sources/CAS_ref_files folder
        You need to do the same with CompTox: get the *synonyms* from a bulk search
        of the new CAS numbers (export into Excel, saved as csv) 
        and put into the CompTox_ref_file directory.
        After that, run make_CAS_ref.py to update the CAS_ref_files and synonym
        files.
        
        *** Then run this script again, then curate it's output
        *****
        
    3)  The company_xlate file has been updated with new names and is ready for
        the CURATION step. Find it as /tmp/company_xlateNEW.csv IF THERE ARE
        ANY TO CURATE.
        
    4)  When finished curating, move to /data/transformed (be sure to save old one as backup)
        and move on to the next step.  (After curating and moving, it is a good idea
        to run this again to verify that you caught all that needed to be curated.)

    5)  Now, update all external data sets. In particular, update the CWA and DWSHA
        data sets at CompTox.  Move that excel
        file into the external_refs directory and update the name in the 
        external_dataset_tools.py file.
        
"""

print(s)
# Clean up memory
rawdf = None
master_raw = None
df = None
gc.collect()


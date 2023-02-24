# -*- coding: utf-8 -*-
"""
Created on Sat Oct 30 12:57:54 2021

@author: Gary

Used to create a full set of data, code and documentation that can be
the source of any use of the Open-FF products.  It is meant to
create an anchor that anyone working on the data can refer to.

Provided in the repo are: 
    - data table pickles (for recreating analysis sets)
    - zips of filtered and full sets
    - copies of the translation tables used to created the database
    - a readme file that explains things like when the data were downloaded,
      when the data were compiled, what code version was used to create it,
      etc.
    - maybe a simple script to help user extract their own dataset from the 
      pickles.
    - a csv file of hashes for most of the files in the repo.  These can 
      be used to verify that the files have not been changed since creation.
"""

import os, shutil
from pathlib import Path
from hashlib import sha256
import pandas as pd
import core.Analysis_set as ana_set
import datetime
#import zipfile
import build_common
outdir = build_common.get_pickle_dir()
sources = build_common.get_data_dir()
trans_dir = build_common.get_transformed_dir()
tempfolder = './tmp/'


data_source = 'bulk'  # 'bulk' for typical processing
repo_name = 'v15_beta_2023_02_17'
inc_zipped = False # should be true for major repositories

# data_source = 'NM_scrape_2022_05'
# repo_name = 'NM_scrape_2022_10_09'

if inc_zipped == False:
    repo_name = repo_name + '_min'

repo_dir = build_common.get_repo_dir() + repo_name
pklsource = 'currentData_pickles'

descriptive_notes = f""" This is an OpenFF data repository for the 
bulk download of FracFocus from Jan 14, 2023

Created {datetime.date.today()}
CodeOcean version: 15
Since the version 15 release, this beta version incorporates:
 - The field "rq_lbs" was added from the the ver 15 code.
 - Cleaned up the bgCAS that were not striped of trailing spaces.
 - Using new comptox batch cleanup routines. Old EPA names are no longer
    available.
 - bgLatitude and bgLongitude are now replaced with state-derived values
    when errors are detected in the FracFocus versions of those varibales.
    The field bgLocationSource indictates where the bg values come from.
 - include stLatitude and stLongitude in the disclosure table.
 - added bgFederalWell and bgNativeAmericanWell that is derived from
    the PADUS-3 data set.
 
"""


# =============================================================================
# descriptive_notes = f""" This is an OpenFF data repository for the 
# **New Mexico state-held** data set as scraped by OpenFF in May 2022.  
# 
# NOTE THAT THESE DATA ARE OLD AND MAY HAVE BEEN UPDATED BY THE INDUSTRY
# SINCE THE TIME THEY WERE SCRAPED.  USE THESE DATA WITH CARE AND DISCRETION.
# 
# Created {datetime.date.today()}
# CodeOcean version: 15
# Since the version 15 release, this beta version incorporates:
#  - The field "rq_lbs" was added from the the ver 15 code.
#  - Cleaned up the bgCAS that were not striped of trailing spaces.
#  - Using new comptox batch cleanup routines. Old EPA names are no longer
#      available.
#  - bgLatitude and bgLongitude are now replaced with state-derived values
#      when errors are detected in the FracFocus versions of those varibales.
#      The field bgLocationSource indictates where the bg values come from.
#  - added bgFederalWell and bgNativeAmericanWell that is derived from
#     the PADUS-3 data set
# 
# """
# =============================================================================

boilerplate = """This directory contains a data set generated by the Open-FF
project.
"""
print(descriptive_notes)
print(f'Including big zipped files? {inc_zipped}')
q = input(f'Data source is set to < {data_source} > and repo to {repo_name}. \n\n -- Enter "y" to start building process.  > ')
assert q=='y'

print(f'Starting creation of new Data Repo set: {repo_name}')
# create new directory
try:
    os.mkdir(repo_dir)
except:
    print(f'\nCreation of Directory <{repo_dir}> not allowed;  already created?')

# create and store README
with open(repo_dir+'/README.txt','w') as f:
    f.write(descriptive_notes+'\n')
    f.write(boilerplate)  # see below for the text

# # generate output csv's 
# first try to delete any existing big files
try:
    os.remove(repo_dir+'/standard_filtered.zip')
    print('removed old std_filtered')
except:
    pass
try:
    os.remove(repo_dir+'/full_no_filter.zip')
    print('removed old full')
except:
    pass
try:
    os.remove(repo_dir+'/catalog_set.zip')
    print('removed old catalog_set')
except:
    pass


if inc_zipped:
    # now make the zipped sets
    ana_set.Standard_data_set().save_compressed()
    shutil.move(outdir+'standard_filtered.zip',repo_dir+'/standard_filtered.zip')
    
    ana_set.Full_set().save_compressed()
    shutil.move(outdir+'full_no_filter.zip',repo_dir+'/full_no_filter.zip')
    
    ana_set.Catalog_set().save_compressed()
    shutil.move(outdir+'catalog_set.zip',repo_dir+'/catalog_set.zip')


# copy pickles
pickledir = repo_dir+'/pickles'
try:
    os.mkdir(pickledir)
except:
    print(f'\nDirectory <{pickledir}> not created;  already created?')
flst = os.listdir(outdir+pklsource)
for fn in flst:
    if fn[-4:]=='.pkl':
        if not (fn[-7:]=='_df.pkl'):  # ignore pickled analysis sets
            shutil.copyfile(outdir+pklsource+'/'+fn, pickledir+'/'+fn)
            print(f'copied {fn}')


# copy curation files

cfiles = ['carrier_list_auto.csv',
          'carrier_list_curated.csv',
          'carrier_list_prob.csv']
files = ['CAS_curated.csv',
         'casing_curated.csv','company_xlate.csv','ST_api_without_pdf.csv',
         'ING_curated.csv','CAS_synonyms.csv',
         'CAS_synonyms_CompTox.csv','CAS_ref_and_names.csv',
         'tripwire_summary.csv','upload_dates.csv',
         'missing_values.csv']

cdir = 'curation_files'
os.mkdir(cdir) # made in the cwd.

for fn in files:
    print(f'  - zipping {fn}')
    shutil.copy(trans_dir+fn,cdir)
for fn in cfiles:
    print(f'  - zipping {fn}')
    shutil.copy(trans_dir+f'{data_source}/{fn}',cdir)
shutil.make_archive(os.path.join(repo_dir,cdir),'zip',cdir)
shutil.rmtree(cdir)         

# copy CAS and CompTox reference files

cdir = 'CAS_ref_files'
sdir = r"C:\MyDocs\OpenFF\data\external_refs\CAS_ref_files"

cdir = 'CompTox_ref_files'
sdir = r"C:\MyDocs\OpenFF\data\external_refs\CompTox_ref_files"
shutil.make_archive(os.path.join(repo_dir,cdir),'zip',sdir)

# now create hashfile
#  this is a pandas df with all files (except the hashfile) in the "filename"
#  field and the sha256 hash of the file in the "sha256" field.
#  These hashes can be used to verify that the data is in the original state and
#  has not been modified.

print('\nMaking file hashes for validation')

to_hash = ['pickles/bgCAS.pkl',
           'pickles/cas_ing.pkl',
           'pickles/chemrecs.pkl', 
           'pickles/companies.pkl',
           'pickles/disclosures.pkl',
           'curation_files.zip',                                        
           'CAS_ref_files.zip',
           'CompTox_ref_files.zip',
           'README.txt',
           'standard_filtered.zip',
           'full_no_filter.zip',
           'catalog_set.zip']

fnout = []; fnhash = []
for fn in to_hash:
    path = Path(os.path.join(repo_dir,fn))
    if path.is_file():
        print('  -- '+fn)
        fnout.append(fn)
        with open(path,'rb') as f:
            fnhash.append(sha256(f.read()).hexdigest())
    else:
        print(f'  >> file not in repo: {fn} <<')
pd.DataFrame({'filename':fnout,'filehash':fnhash}).to_csv(repo_dir+'/filehash.csv',
                                                          index=False)

print(f'Repo creation completed: {repo_dir}')
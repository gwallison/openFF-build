# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 06:50:50 2022

@author: Gary

This script is used to extract data from a comptox batch search into work files
for the build process.

"""
import pandas as pd
import os

fn = r"C:\MyDocs\OpenFF\src\openFF-build\tmp\comptox_batch_results.xlsx"
comptox_dir = r"C:\MyDocs\OpenFF\data\external_refs\CompTox_ref_files"
ext_ref_dir = r"C:\MyDocs\OpenFF\data\external_refs"
t = pd.read_excel(fn, engine="openpyxl",sheet_name="Main Data")
t = t.rename({'INPUT':'bgCAS','PREFERRED_NAME':'epa_preferred_name',
              'IUPAC_NAME':'iupac_name'},axis=1)
c = t.epa_preferred_name.notna()
t = t[c]
t.to_csv(os.path.join(ext_ref_dir,'comptox_name_list.csv'),quotechar='$',encoding='utf-8')

st = pd.read_excel(fn, engine="openpyxl",sheet_name="Synonym Identifier")
st.columns = ['epa_preferred_name','identifier','dummy']
# st.head()
mg = pd.merge(t[['bgCAS','epa_preferred_name']],st[['epa_preferred_name','identifier']],
              on='epa_preferred_name',how='left')
print(mg.head(1).T)
mg.to_csv(os.path.join(comptox_dir,'current_batch_synonyms.csv'),
          quotechar='$',encoding='utf-8')
# mg.to_csv(os.path.join(comptox_dir,'broad_search_synonyms.csv'),
#           quotechar='$',encoding='utf-8')
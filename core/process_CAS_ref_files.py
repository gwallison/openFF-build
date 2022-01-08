# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 19:41:04 2019

@author: GAllison

This set of routines is used to translate the ref files created with SciFinder
(online) to a reference dictionary used to validate the CASNumbers and 
find accepted synonyms for IngredientNames in the FracFocus dataset.

The ref files that are used as INPUT to this routine are created by 
searching manually for a given CAS number (the 
SciFinder website has heavy restrictions on automated searches) and then saving
the SciFinder results to a text file.  The routines below parse those 
text files for the infomation used later when comparing to CASNumber codes in 
FracFocus.  Each ref file may contain several CAS-registry records; that is an
artifact of how we performed the manual searches, and is handled by the code.

Note that 'primary name' is the name used by CAS as the main name for
a material; it is the first entry in the list of synonyms.


"""
import pandas as pd
import numpy as np
import os
import csv
import io
#import pickle
#import core.lsh_tools as lsh


# inputdir = './sources/CAS_ref_files/'
# outputdir = './sources/'  # store in sources because it is used from there
inputdir = 'c:/MyDocs/OpenFF/data/external_refs/CAS_ref_files/'
outputdir = 'c:/MyDocs/OpenFF/data/transformed/'

encod = 'utf-8'

def processRecord(rec):
    """return tuple of (cas#,[syn],[dep])"""
    cas = 'Nope'
    prime = 'empty'
    lst = [] # for synonyms
    dellst = [] # for deprecated cas numbers
    fields = rec.split('FIELD ')
    for fld in fields:
        if 'Registry Number:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            cas = fld[start:end]
        if 'CA Index Name:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            prime = fld[start:end].lower()
        if 'Other Names:' in fld:
            start = fld.find(':')+1
            lst = fld[start:].split(';')
        if 'Deleted Registry Number(s):' in fld:
            start = fld.find(':')+1
            dellst = fld[start:].split(',')
    olst = [prime]
    for syn in lst:
        syn = syn.strip().lower()
        if len(syn)>0: 
            if syn not in olst:
                olst.append(syn)
                
    return (cas,olst,dellst)
   

def processFile(fn,ref_dict):
    with io.open(fn,'r',encoding=encod) as f:
        whole = f.read()
        
    #with io.open(outputdir+'testout.txt','w',encoding=encod) as f:
    #    f.write(whole)
        
    # make sure it looks like the correct format
    if whole[:12]!='START_RECORD':
        print(f'Looks like file: {fn} may not be "tagged" format!')
        print(whole[:15])
        print('ignored...')
        return ref_dict
    records = whole.split('END_RECORD')
    for rec in records:
        tup = processRecord(rec)
        ref_dict[tup[0]] = [tup[1],tup[2]]   
    return ref_dict

def processAll(inputdir=inputdir):
    print('Translating SciFinder output to format useful in Open-FF')
    cas_ref_dict = {}
    fnlst = os.listdir(inputdir)
    for fn in fnlst:
        cas_ref_dict = processFile(inputdir+fn,cas_ref_dict)
    print(f'Number of CAS references collected: {len(cas_ref_dict)}')
    casl = list(cas_ref_dict.keys())
    
    # first produce the cas# : ingredName file
    namel = []
    for cas in casl:
        namel.append(cas_ref_dict[cas][0][0])  # take first name as primary
    df = pd.DataFrame({'cas_number':casl,'ing_name':namel})
    # We use '$' to quote all fields because of the extensive use of
    #   punctuation characters in the chemical name fields.
    df.to_csv(outputdir+'CAS_ref_and_names.csv',quotechar='$',
              quoting=csv.QUOTE_ALL,index=False,encoding = encod)
    
    #n Next produce the synonym file: synon : cas#
    synl = []
    cas_for_syn = []
    for cas in casl:
        for syn in cas_ref_dict[cas][0]:
            synl.append(syn)
            cas_for_syn.append(cas)
    df = pd.DataFrame({'synonym':synl,'cas_number':cas_for_syn})
    # We use '$' to quote all fields because of the extensive use of
    #   punctuation characters in the chemical name fields.
    df.to_csv(outputdir+'CAS_synonyms.csv',quotechar='$',
              quoting=csv.QUOTE_ALL,index=False, encoding = encod)
    print(f'Number of synonyms: {len(synl)}')
    
    #n Next produce the deprecated file: dep_cas : cas#
    depl = []
    cas_for_dep = []
    for cas in casl:
        for dep in cas_ref_dict[cas][1]:
            t = dep.strip()
            if  t!='':
                depl.append(t)    
                cas_for_dep.append(cas)
    df = pd.DataFrame({'deprecated':depl,'cas_replacement':cas_for_dep})
    df.to_csv(outputdir+'CAS_deprecated.csv',quotechar='$',
              quoting=csv.QUOTE_ALL,index=False, encoding = encod)
    print(f'Number of deprecated: {len(depl)}')
    
    # finally return the dictionary to the calling function
    return(cas_ref_dict)

def remove_final_paren(s):
    lst = s.split(' ')
    last = lst[-1]
    if (last[0] == '(') & (last[-1]==')'):
        return s[:-(len(last)+1)]
    return s

# =============================================================================
# def create_syn_lsh_pkl():
#     """ used to recreate the lsh object that we use to see if
#     a raw IngredientName is a good match for bgCAS
#     Run this after adding new chemicals to the CAS ref"""
#     
#     df = pd.read_csv(outputdir+'CAS_synonyms.csv',quotechar='$',
#                      encoding = encod)
#     df= df[~(df.synonym.isna())]
#     syns = df.synonym.unique().tolist()
#     print(f'Fetched CAS synonyms: {len(syns)}')
#     print('Creating synonym LSH pickle - be patient')
#     pkl = lsh.LSH_set(rawlist=syns,
#                       thresholds=[1.0,0.9,0.8]) #0.7,0.6,0.5])
#     with open(outputdir+'lsh_synonym.pkl','wb') as f:
#         pickle.dump(pkl,f)
# 
#     # now make version without final parenthetical phrase
#     df.synonym = df.synonym.map(lambda x: remove_final_paren(x))
#     syns = df.synonym.unique().tolist()
#     print('Creating synonym LSH pickle WITHOUT final paren - be patient')
#     pkl = lsh.LSH_set(rawlist=syns,
#                       thresholds=[1.0,0.9,0.8]) #0.7,0.6,0.5])
#     with open(outputdir+'lsh_synonym_no_final_paren.pkl','wb') as f:
#         pickle.dump(pkl,f)
# 
# =============================================================================
# =============================================================================
# def create_CompTox_syn_lsh_pkl():
#     """ used to recreate the lsh object that we use to see if
#     a raw IngredientName is a good match for bgCAS
#     Run this after adding new chemicals to the COMPTOX CAS ref"""
#     
#     df = pd.read_csv(outputdir+'CAS_synonyms_CompTox.csv',quotechar='$',
#                      encoding = encod)
#     df= df[~(df.synonym.isna())]
#     syns = df.synonym.unique().tolist()
#     print(f'Fetched CAS synonyms (comptox): {len(syns)}')
#     print('Creating synonym LSH pickle - be patient')
#     pkl = lsh.LSH_set(rawlist=syns,
#                       thresholds=[1.0,0.9,0.8]) #0.7,0.6,0.5])
#     with open(outputdir+'lsh_synonym_comptox.pkl','wb') as f:
#         pickle.dump(pkl,f)
# 
# =============================================================================

# =============================================================================
# def get_syn_lsh_obj(fn='lsh_synonym.pkl'):
#     with open(outputdir+fn,'rb') as f:
#         return pickle.load(f)
#     
# =============================================================================
def add_syn_to_dict(row,dic):
    dic.setdefault(row.synonym, []).append(row.cas_number)
    return dic


def get_CAS_syn_dict(remove_paren=False):
    df = pd.read_csv(outputdir+'CAS_synonyms.csv',quotechar='$',
                     encoding = encod)
    df= df[~(df.synonym.isna())]
    if remove_paren:
        # used to drop parenthetical phrase at end of synonym. Often these
        # are not part of a typical name and so makes matching harder.
        df.synonym = df.synonym.map(lambda x: remove_final_paren(x))
        
    cdic = {}
    for i,row in df.iterrows():
        cdic = add_syn_to_dict(row, cdic)
    return cdic

def make_syn_list(rec,big):
    lst = rec.split('|')
    for i in lst[1:]:
        big.append('$'+lst[0].strip()+'$,$'+i.strip().lower()+'$\n')
    return big

    
def make_CompTox_syn_file():
    raw_ct = []
    refdir = 'c:/MyDocs/OpenFF/data/external_refs/CompTox_ref_files/'
    flst = os.listdir(refdir)
    for fn in flst:
        if fn[-4:] != '.csv':
            print(f'NON csv file in CompTox references {fn}')
        else:
            t = pd.read_csv(refdir+fn)
            print(f'Reading {fn}, len: {len(t)}')
            raw_ct.append(t)
    syn = pd.concat(raw_ct,sort=True)
    big = ['$cas_number$,$synonym$\n']
    for i,row in syn.iterrows():
        if row.IDENTIFIER != np.NaN:
            big = make_syn_list(row.IDENTIFIER,big)
        #print(i,len(big))
    
    with open('c:/MyDocs/OpenFF/data/transformed/CAS_synonyms_CompTox.csv','w',encoding='utf-8') as f:
        for i in big:
            f.write(i)
        
if __name__ == '__main__':
    #create_CompTox_syn_lsh_pkl()    
    #dic = processAll()
    pass
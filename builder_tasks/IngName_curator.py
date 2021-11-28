# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 09:54:59 2021

@author: Gary

This set of routines is used to assist in the curation of IngredientName.

"""
import numpy as np
import pandas as pd
import difflib as dl

# nonspdf = pd.read_csv('./sources/IngName_non-specific_list.csv',quotechar='$',
#                       encoding='utf-8')
# ctsyndf = pd.read_csv('./sources/CAS_synonyms_CompTox.csv',quotechar='$',
#                       encoding='utf-8')
# ctsyndf = ctsyndf[~ctsyndf.duplicated()]
# sfsyndf = pd.read_csv('./sources/CAS_synonyms.csv',quotechar='$',
#                       encoding='utf-8')

# ING_to_curate = pd.read_csv('./tmp/ING_to_curate.csv',quotechar='$',encoding='utf-8')
# ING_curated = pd.read_csv('./sources/ING_curated.csv',quotechar='$',encoding='utf-8')
# fullscan_df = pd.read_csv('./sources/ING_fullscan.csv',quotechar='$',encoding='utf-8')

# t = pd.merge(ING_to_curate,ING_curated[['IngredientName']],on='IngredientName',
#              how='outer',indicator=True)
# ING_to_curate = t[t['_merge']=='left_only']

# print(f'Number of Names to curate: {len(ING_to_curate)}')

# create ref dictionary
#  Text/syn : (curation_code, [(casnum,source), casnum,source)])

def add_to_ref(dic,txt,curcode,casnum,source):
    if txt=='': # don't add empty strings
        return dic
        
    if txt in dic.keys():
        if len(txt)<1:
            print(txt)
        # add to existing entry. NOTE THAT LAST INSTANCE FOR AN ENTRY IS
        #   THE GIVEN PRIMACY by writing over previous curcodes
        prev = dic[txt]
        new = prev[1]
        new.append((casnum,source))
        dic[txt] = (curcode,new)
    else:
        dic[txt] = (curcode,[(casnum,source)])
    return dic

def summarize_refs(ref):
    for item in ref:
        lst = ref[item][1]
        lbl = ref[item][0]
        if len(lst)>1:  # may need to adjust curation code
            first = lst[0][0]
            for l in lst[1:]:
                if l[0]!=first:
                    lbl = 'non_spec'
                    ref[item] = (lbl, lst)
                    #print(f'adjusted {item} to non_specific')
                    break
    return ref

def ref_stats(dic):
    cntr =0
    for item in dic:
        if len(dic[item][1])>1:
            cntr+=1
    return cntr

def build_refdic():
    refdic = {}
    nonspdf = pd.read_csv('./sources/IngName_non-specific_list.csv',quotechar='$',
                      encoding='utf-8')
    ctsyndf = pd.read_csv('./sources/CAS_synonyms_CompTox.csv',quotechar='$',
                          encoding='utf-8')
    ctsyndf = ctsyndf[~ctsyndf.duplicated()]
    sfsyndf = pd.read_csv('./sources/CAS_synonyms.csv',quotechar='$',
                      encoding='utf-8')
    # build refdic, one ref set at a time
    print('scifinder to refdic...')
    for i,row in sfsyndf.iterrows():
        refdic = add_to_ref(refdic, row.synonym, 'CASsyn', row.cas_number, 'scifinder')
    print('comptox to refdic...')
    for i,row in ctsyndf.iterrows():
        refdic = add_to_ref(refdic, row.synonym, 'CASsyn', row.cas_number, 'comptox')
    print('nonspec to refdic...')
    for i,row in nonspdf.iterrows():
        refdic = add_to_ref(refdic, row.non_specific_code, 'non_spec', np.NaN, row.source)
    print(f'Len of refdic {len(refdic)}, num of multiple refs/item: {ref_stats(refdic)}')
    
    return summarize_refs(refdic)

def add_curated_record(record,ING_curated):
    t = pd.concat([record,ING_curated],sort=True)
    return t

def save_curated_df(ING_curated):
    t = ING_curated[['IngredientName','recog_syn','prospect_CAS_fromIng','syn_code','match_ratio',
                     'alt_CAS','first_date','change_date','change_comment',
                     ]]
    t.to_csv('./tmp/ING_curated_NEW.csv',index=False,quotechar="$",encoding='utf-8')
    return t

def make_record(IngN='unk',recog_syn='unk',prospect_CAS='unk',syn_code='unk',match_ratio=0,
                alt1_CAS='unk',alt1_syn='unk'):
    return pd.DataFrame({'IngredientName':[IngN],
                         'is_new':True,
                         'recog_syn':[recog_syn],
                         'prospect_CAS_fromIng':[prospect_CAS],
                         'syn_code':[syn_code],
                         'match_ratio':[match_ratio],
                         'alt_CAS':[alt1_CAS]})


def add_fullscan_record(record,fs_df):
    t = pd.concat([record,fs_df],sort=True)
    t = t[['IngredientName','recog_syn','prospect_CAS','syn_code','match_ratio']]
    t.to_csv('./tmp/ING_fullscan_NEW.csv',index=False,quotechar="$",encoding='utf-8')
    return t

def make_fullscan_record(IngN='unk',recog_syn='unk',prospect_CAS='unk'
                         ,syn_code='unk',match_ratio=0):
    return pd.DataFrame({'IngredientName':[IngN],
                      'recog_syn':[recog_syn],
                      'prospect_CAS':[prospect_CAS],
                      'syn_code':[syn_code],
                      'match_ratio':[match_ratio]},)

def full_scan(to_curate,refdic,fs_df):
    # used to scan the classify the entire list of IngNames.  This will take
    # a significant amt of time for each entry.
    ingwords = to_curate[to_curate.IngredientName.notna()].IngredientName.tolist()
    ref = list(refdic.keys())
    for cntr,i in enumerate(ingwords):
        print(f'\n\n< {i} >')
        d = dl.get_close_matches(i, ref,cutoff=0.85,n=3) # 
        if len(d)==0:
            print(f'No matches  ({cntr})')
            rec = make_fullscan_record(i,'no match','non_spec','non_spec',0)
            fs_df = add_fullscan_record(rec, fs_df)
        if len(d)>0:
            for cnt,match in enumerate(d):
                print(f'match {cnt}  ({cntr})')
                print(refdic[match])
                mat = refdic[d[cnt]]
                ratio = dl.SequenceMatcher(a=i,b=match).ratio() 
                if mat[0] == 'non_spec':
                    rec = make_fullscan_record(i,d[cnt],'non_spec','non_spec',ratio)
                else:
                    rec = make_fullscan_record(i,d[cnt],mat[1][0][0],mat[0],ratio)
                fs_df = add_fullscan_record(rec, fs_df)
    return fs_df
    
def analyze_fullscan(fs_df,ING_curated): #,useNEW=True):
    # if useNEW:
    #     fs_df = pd.read_csv('./tmp/ING_fullscan_NEW.csv',quotechar='$',encoding='utf-8')

    gb1 = fs_df.groupby('IngredientName',as_index=False)['match_ratio'].max()
    gb1.columns = ['IngredientName','max_ratio']
    fs_df = pd.merge(fs_df,gb1,on='IngredientName',how='left')

    no_match = gb1[gb1.max_ratio<0.95].IngredientName.unique().tolist()
    perfect = gb1[gb1.max_ratio==1].IngredientName.unique().tolist()


    # ---- store no_matches:
    print('parsing Names with no matches')
    for ing in no_match:
        record = make_record(IngN=ing,prospect_CAS='non_spec',syn_code='no_match')
        ING_curated = add_curated_record(record, ING_curated)
        
    # ---- store perfect matches:
    print('parsing Names with perfect matches')
    gb = fs_df[fs_df.match_ratio==1].groupby('IngredientName',as_index=False)['prospect_CAS'].first()
    gb.columns = ['IngredientName','topCAS']
    fs2 = pd.merge(fs_df,gb,on='IngredientName',how='left')
    c1 = fs2.prospect_CAS!=fs2.topCAS
    c2 = fs2.match_ratio>=0.95    
    gb2 = fs2[c1&c2].groupby('IngredientName')['prospect_CAS'].apply(set).apply(list).reset_index()
    gb2.columns = ['IngredientName','alt1_CAS']
    fs2 = pd.merge(fs2,gb2,on='IngredientName',how='left')
    for i,row in fs2[fs2.match_ratio==1].iterrows():
        record = make_record(IngN=row.IngredientName,
                             prospect_CAS=row.prospect_CAS,
                             recog_syn = row.recog_syn,
                             syn_code='perfect',
                             match_ratio=row.match_ratio,
                             alt1_CAS=row.alt1_CAS)
        ING_curated = add_curated_record(record, ING_curated)
        
    # ---- find and store matches that are less than perfect
    print('parsing close and conflicting matches')
    c1 = fs_df.IngredientName.isin(no_match)
    c2 = fs_df.IngredientName.isin(perfect)
    fs3 = fs_df[(~c1)&(~c2)].copy() # the rest of the list

    gb = fs3.groupby(['IngredientName','prospect_CAS'],as_index=False)['match_ratio'].max()
    gb1 = gb.groupby('IngredientName',as_index=False)['match_ratio'].max()
    gb1.columns = ['IngredientName','max_ratio']
    mg = pd.merge(gb,gb1,on='IngredientName',how='left')
    mg['ratdiff'] = mg.max_ratio-mg.match_ratio
    #mg = mg[mg.match_ratio!=mg.max_ratio]
    mg = mg[mg.ratdiff<0.05]
    gbwithin = mg.groupby('IngredientName',as_index=False)['prospect_CAS'].count()
    gbwithin.columns = ['IngredientName','numprospect']
    #mg = pd.merge(mg,gbwithin,on='IngredientName',how='left')
    gbalt = mg.groupby('IngredientName')['prospect_CAS'].apply(list).reset_index()
    gbalt.columns = ['IngredientName','alt1_CAS']

    t = fs3.sort_values('match_ratio', ascending=False).drop_duplicates('IngredientName')
    t = pd.merge(t,gbalt,on='IngredientName',how='left')

    t = fs3.sort_values('match_ratio', ascending=False).drop_duplicates('IngredientName')
    t = pd.merge(t,gbalt,on='IngredientName',how='left')
    t = pd.merge(t,gbwithin,on='IngredientName',how='left')
    #print(t.columns)
    t.alt1_CAS = np.where(t.numprospect==1,np.NaN,t.alt1_CAS)
    t.syn_code = np.where(t.numprospect==1,'close_match','conflicting_matches')
    t.prospect_CAS = np.where(t.numprospect==1,t.prospect_CAS,'non_spec')
    for i,row in t.iterrows():
        record = make_record(IngN=row.IngredientName,
                             prospect_CAS=row.prospect_CAS,
                             recog_syn = row.recog_syn,
                             syn_code=row.syn_code,
                             match_ratio=row.match_ratio,
                             alt1_CAS=row.alt1_CAS)
        ING_curated = add_curated_record(record, ING_curated)

    #finish up
    save_curated_df(ING_curated)
    
    
#refdic = build_refdic(refdic)
#refdic = summarize_refs(refdic)
#fullscan_df = full_scan(ING_to_curate,refdic,fullscan_df)
#analyze_fullscan(fullscan_df, ING_curated,useNEW=True)
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 09:32:29 2019

@author: Gary
"""
import pandas as pd
import numpy as np

# =============================================================================
# def add_Elsner_table(df,sources='./sources/',
#                      outdir='./out/',
#                      ehfn='elsner_corrected_table.csv'):
#     #print('Adding Elsner/Hoelzer table to CAS table')
#     ehdf = pd.read_csv(sources+ehfn,quotechar='$')
# # =============================================================================
# #     # checking overlap first:
# #     ehcas = list(ehdf.eh_CAS.unique())
# #     dfcas = list(df.bgCAS.unique())
# #     with open(outdir+'elsner_non_overlap.txt','w') as f:
# #         f.write('**** bgCAS numbers without an Elsner entry: *****\n')
# #         for c in dfcas:
# #             if c not in ehcas:
# #                 f.write(f'{c}\n')
# #         f.write('\n\n***** Elsner CAS numbers without a FF entry: *****\n')
# #         for c in ehcas:
# #             if c not in dfcas:
# #                 f.write(f'{c}\n')
# # 
# # =============================================================================
#     mg = pd.merge(df,ehdf,left_on='bgCAS',right_on='eh_CAS',
#                   how='left',validate='1:1')
#     return mg
# =============================================================================

# =============================================================================
# def add_WellExplorer_table(df,sources='./sources/',
#                      outdir='./out/',
#                      wefn='well_explorer_corrected.csv'):
#     """Add the WellExplorer data table. """
#     #print('Adding WellExplorer table to CAS table')
#     wedf = pd.read_csv(sources+wefn)
#     #print(wedf.head())
#     # checking overlap first:
# # =============================================================================
# #     wecas = list(wedf.we_CASNumber.unique())
# #     dfcas = list(df.bgCAS.unique())
# #     with open(outdir+'wellexplorer_non_overlap.txt','w') as f:
# #         f.write('**** bgCAS numbers without an WellExplorer entry: *****\n')
# #         for c in dfcas:
# #             if c not in wecas:
# #                 f.write(f'{c}\n')
# #         f.write('\n\n***** WellExplorer CAS numbers without a FF entry: *****\n')
# #         for c in wecas:
# #             if c not in dfcas:
# #                 f.write(f'{c}\n')
# # 
# # =============================================================================
#     mg = pd.merge(df,wedf,left_on='bgCAS',right_on='we_CASNumber',
#                   how='left',validate='1:1')
#     return mg
# =============================================================================

    
def add_TEDX_ref(df,sources='./sources/',
                 tedx_fn = 'TEDX_EDC_trimmed.xls'):
    #print('Adding TEDX link to CAS table')
    tedxdf = pd.read_excel(sources+tedx_fn)
    tedx_cas = tedxdf.CAS_Num.unique().tolist()
    df['is_on_TEDX'] = df.bgCAS.isin(tedx_cas)
    return df
    
# =============================================================================
# def add_TSCA_ref(df,sources='./sources/',
#                  tsca_fn = 'TSCAINV_092019.csv'):
#     #print('Adding TSCA to CAS table')
#     tscadf = pd.read_csv(sources+tsca_fn)
#     tsca_cas = tscadf.CASRN.unique().tolist()
#     df['is_on_TSCA'] = df.bgCAS.isin(tsca_cas)
#     return df
# =============================================================================
    
def add_Prop65_ref(df,sources='./sources/',
                 p65_fn = 'p65list12182020.csv'):
    #print('Adding California Prop 65 to CAS table')
    p65df = pd.read_csv(sources+p65_fn,encoding='iso-8859-1')
    p65_cas = p65df['CAS No.'].unique().tolist()
    df['is_on_prop65'] = df.bgCAS.isin(p65_cas)
    return df
    
# =============================================================================
# def add_CWA_primary_ref(df,sources='./sources/',
#                      cwa_fn = 'sara_sdwa_cwa.csv'):
#     # this file is used to provide the CWA priority list
#     #print('Adding SDWA/CWA lists to CAS table')
#     cwadf = pd.read_csv(sources+cwa_fn)
#     cwadf = cwadf[cwadf.Legislation=='CWA']  # keep only CWA
#     cwa_cas = cwadf['CASNo'].unique().tolist()
#     df['is_on_EPA_priority'] = df.bgCAS.isin(cwa_cas)
#     return df
# =============================================================================

def add_diesel_list(df):
    print('  -- processing epa diesel list')
    cas = ['68334-30-5','68476-34-6','68476-30-2','68476-31-3','8008-20-6']
    df['is_on_diesel'] = df.bgCAS.isin(cas)
    return df

def add_UVCB_list(df,sources='./sources/'):
    print('  -- processing TSCA UVCB list')
    uvcb = pd.read_csv(sources+'TSCA_UVCB_202202.csv')
    cas = uvcb.CASRN.unique().tolist()
    df['is_on_UVCB'] = df.bgCAS.isin(cas)
    return df

def add_NPDWR_list(df,sources='./sources/'):
    # add list curated by Angelica
    print('  -- processing NPDWR list')
    npdwr = pd.read_csv(sources+'NationalPrimaryDrinkingWaterRegulations_machine_readable_FEB2022.csv')
    cas = npdwr[npdwr.CASRN.notna()].CASRN.unique().tolist()
    df['is_on_NPDWR'] = df.bgCAS.isin(cas)
    return df

def add_RQ_list(df,sources='./sources/'):
    # variable added to some bgCAS is 'rq_lbs'
    print('  -- processing Reportable Quantity list')
    rq = pd.read_csv(sources+'RQ_final.csv',quotechar='$',encoding='utf-8')
    df = pd.merge(df,rq,on='bgCAS',how='left')
    return df
    

def add_CompTox_refs(df,sources='./sources/'):
    
    ctfiles = {'CWA': 'Chemical List CWA311HS-2022-03-31.csv',
               'DWSHA' : 'Chemical List EPADWS-2022-03-31.csv',
               'AQ_CWA': 'Chemical List WATERQUALCRIT-2022-03-31.csv',
               'HH_CWA': 'Chemical List NWATRQHHC-2022-03-31.csv',
               'IRIS': 'Chemical List IRIS-2022-03-31.csv',
               'PFAS_list': 'Chemical List PFASMASTER-2022-04-01.csv',
               'volatile_list': 'Chemical List VOLATILOME-2022-04-01.csv'}
    reffn = 'CCD-Batch-Search_2022-04-01_12_32_54.csv'
    for lst in ctfiles.keys():
        print(f'  -- processing {lst}')
        ctdf = pd.read_csv(sources+ctfiles[lst],low_memory=False,
                           dtype={'CASRN':'str'})
#        ctdf['DTXSID'] = ctdf.DTXSID.str[-13:]
        clst= ctdf.CASRN.unique().tolist()
        df['is_on_'+lst] = df.bgCAS.isin(clst)
        
    # now add the epa ref numbers and names
    refdf = pd.read_csv(sources+reffn)
    refdf = refdf.rename({'INPUT':'bgCAS','PREFERRED_NAME':'epa_pref_name'},axis=1)
    refdf = refdf[~refdf.bgCAS.duplicated()] # get rid of double callouts
    refdf.DTXSID = np.where(refdf.DTXSID.isna(),
                            refdf.FOUND_BY,
                            refdf.DTXSID)
    df = pd.merge(df,refdf[['bgCAS','DTXSID','epa_pref_name']],
                  how='left',on='bgCAS')
    #df.to_csv('./tmp/bgcas_test.csv')
    return df
       
# def add_CompTox_refs_old(df,sources='./sources/',
#                      ct_fn = 'CompTox_v2021_09_17.xls'):
#     # this file is used to provide the CWA priority list
#     #print('Adding SDWA/CWA lists to CAS table')
#     ctdf = pd.read_excel(sources+ct_fn)
#     ctdf = ctdf.rename({'INPUT':'bgCAS','EPADWS':'is_on_DWSHA',
#                         'CWA311HS':'is_on_CWA',}, axis=1)
#     df = pd.merge(df,ctdf[['bgCAS','is_on_CWA','is_on_DWSHA','DTXSID']],
#                   on='bgCAS',how='left')
#     df.is_on_CWA = np.where(df.is_on_CWA=='Y',True,False)
#     df.is_on_DWSHA = np.where(df.is_on_DWSHA=='Y',True,False)
#     df.DTXSID = np.where(df.DTXSID=='-',np.NaN,df.DTXSID)
#     return df

# replaced by the comptox PFAS master list
# def add_PFAS_ref(df,sources='./sources/',
#                      pfas_fn = 'EPA_PFAS_list_simplified.csv'):
#     #print('Adding SDWA/CWA lists to CAS table')
#     pfasdf = pd.read_csv(sources+pfas_fn)
#     pfas_cas = pfasdf['CASRN'].unique().tolist()
#     df['is_on_PFAS_list'] = df.bgCAS.isin(pfas_cas)
#     return df

# replaced by comptox volatilome list
# def add_Volatile_ref(df,sources='./sources/',
#                      volatile_fn = '40 CFR 59 Part E National Volatile Organic Compound Emission 20210313-205641.xls'):
#     #print('Adding SDWA/CWA lists to CAS table')
#     voldf = pd.read_excel(sources+volatile_fn)
#     voldf = voldf[voldf.CAS.notna()]
#     vol_cas = voldf['CAS'].unique().tolist()
#     #print(f'Volatile cas#: {vol_cas}')
#     df['is_on_volatile_list'] = df.bgCAS.isin(vol_cas)
#     return df


    
def add_all_bgCAS_tables(df,sources='./sources/external_refs/',
                         outdir='./outdir/'):
    #df = add_CWA_primary_ref(df,sources) 
    df = add_CompTox_refs(df,sources)
    df = add_NPDWR_list(df,sources)
    df = add_Prop65_ref(df,sources)
    #df = add_TSCA_ref(df,sources)
    df = add_TEDX_ref(df,sources)
    #df = add_WellExplorer_table(df,sources,outdir)
    #df = add_Elsner_table(df,sources,outdir)
    #df = add_PFAS_ref(df,sources)
    #df = add_Volatile_ref(df,sources)
    df = add_diesel_list(df)
    df = add_UVCB_list(df,sources)
    df = add_RQ_list(df,sources)
    return df
    
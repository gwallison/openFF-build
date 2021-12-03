# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:19:41 2019

@author: Gary
"""
import pandas as pd
import numpy as np
import gc
import os
import datetime
import core.mass_tools as mt
import core.cas_tools as ct
import core.external_dataset_tools as et

class Table_constructor():
    
    def __init__(self,pkldir='./tmp/',sources='./sources/',
                 outdir = './out/'):
        self.pkldir = pkldir
        self.sources = sources
        self.trans_dir = self.sources+'transformed/'
        self.outdir = outdir
        self.tables = {'disclosures': None,
                       'chemrecs': None,
                       'cas_ing': None,
                       'bgCAS': None,
                       'companies': None # all company data is also in records/disc
                       }

        self.pickle_fn = {'disclosures': self.pkldir+'disclosures.pkl',
                          'chemrecs': self.pkldir+'chemrecs.pkl',
                          'cas_ing': self.pkldir+'cas_ing.pkl',
                          #'cas_ing_xlate': self.pkldir+'cas_ing_xlate.pkl',
                          'bgCAS': self.pkldir+'bgCAS.pkl',
                          'companies': self.pkldir+'companies.pkl'
                          }

        self.cas_ing_fn = self.trans_dir+'casing_curated.csv'
        self.cas_ing_source = pd.read_csv(self.cas_ing_fn,quotechar='$',encoding='utf-8')


        self.ST_without_pdf = pd.read_csv(self.trans_dir+'ST_api_without_pdf.csv',
                                          dtype={'api10':'str'})
        
        self.location_ref_fn = self.trans_dir+'uploadKey_ref.csv'
        self.loc_ref_df = pd.read_csv(self.location_ref_fn,quotechar='$',
                                      encoding='utf-8')
        dates = pd.read_csv(self.trans_dir+'upload_dates.csv')

        self.loc_ref_df = pd.merge(self.loc_ref_df,dates[['UploadKey','date_added']],
                           on='UploadKey',how='left',validate='1:1')


    def print_step(self,txt,indent=0,newlinefirst=False):
        if newlinefirst:
            print()
        s = ''
        if indent>0:
            s = '   '*indent
        print(f' {s}-- {txt}')
        
    def print_size(self,df,name='table'):
        rows,cols = df.shape
        self.print_step(f'{name:15}: rows: {rows:7}, cols: {cols:3}',1)


    def assemble_cas_ing_table(self):
        self.print_step('assembling CAS/IngredientName table')
        df = self.cas_ing_source[['CASNumber','IngredientName',
                                  'bgCAS','categoryCAS','syn_code',
                                  'bgSource','alt_CAS']]
        self.tables['cas_ing'] = df
        ct.na_check(df,txt='assembling cas_ing table')

        
    def assemble_companies_table(self):
        self.print_step('assembling companies table')
        self.tables['companies'] = pd.read_csv(self.trans_dir+'company_xlate.csv',
                                                  encoding='utf-8',quotechar='$')\
                                      .drop(['first_date',
                                             'change_date','change_comment'],axis=1)
        
    def assemble_bgCAS_table(self,cas_ing):
        self.print_step('assembling bgCAS table')
        ref = pd.read_csv(self.trans_dir+'CAS_ref_and_names.csv',
                          quotechar='$',encoding='utf-8')
        ref.columns=['bgCAS','bgIngredientName']
        df = pd.DataFrame({'bgCAS':cas_ing.bgCAS.unique().tolist()})
        df = pd.merge(df,ref,on='bgCAS',how='left')
        
        self.print_step('add external references such as TEDX and PFAS',1)
        ext_sources_dir = self.sources+'external_refs/'
        df = et.add_all_bgCAS_tables(df,sources=ext_sources_dir,
                                     outdir=self.outdir)
        self.tables['bgCAS'] = df


    #########   DISCLOSURE TABLE   ################
    def make_date_fields(self,df):
        self.print_step('constructing dates',1)
        # drop the time portion of the datatime
        df['d1'] = df.JobEndDate.str.split().str[0]
        # fix some obvious typos that cause hidden problems
        df['d2'] = df.d1.str.replace('3012','2012')
        df['d2'] = df.d2.str.replace('2103','2013')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(df.d2.unique())})
        tab['date'] = pd.to_datetime(tab.d2)
        # ... then merge back to whole data set
        df = pd.merge(df,tab,on='d2',how='left',validate='m:1')
        df = df.drop(['d1','d2'],axis=1)
        
        #convert date_added field
        df.date_added = pd.to_datetime(df.date_added)
        df['pub_delay_days'] = (df.date_added-df.date).dt.days
        # Any date_added earlier than 10/2018 is unknown
        refdate = datetime.datetime(2018,10,1) # date that I started keeping track
        df.pub_delay_days = np.where(df.date_added<refdate,
                                     np.NaN,
                                     df.pub_delay_days)# is less recent than refdate
        # any fracking date earlier than 4/1/2011 is before FF started taking data
        refdate = datetime.datetime(2011,4,1) # date that fracfocus started
        df.pub_delay_days = np.where(df.date<refdate,
                                     np.NaN,
                                     df.pub_delay_days)# is less recent than refdate
        return df


    def assemble_disclosure_table(self,raw_df):
        self.print_step('assembling disclosure table')
        df = raw_df.groupby('UploadKey',as_index=False)\
                                [['JobEndDate','JobStartDate','OperatorName',
                                  'APINumber', 'TotalBaseWaterVolume',
                                  'TotalBaseNonWaterVolume','FFVersion','TVD',
                                  'StateNumber','CountyNumber',
                                  #'Latitude','Longitude',
                                  'Projection',
                                  'WellName','FederalWell','IndianWell',
                                  'data_source']].first()
        
        self.print_step('create bgOperatorName',1)
        cmp = self.tables['companies'][['rawName','xlateName']]
        cmp.columns = ['OperatorName','bgOperatorName']
        df = pd.merge(df,cmp,on='OperatorName', how='left')

        unOp = df[df.bgOperatorName.isna()]
        if len(unOp)>0: flag = '<******'
        else: flag= ''
        self.print_step(f'Number uncurated Operators: {len(unOp)} {flag}',2)

        df = pd.merge(df,self.loc_ref_df,on='UploadKey',how='left',
                      validate='1:1')
        df = self.make_date_fields(df)

        self.tables['disclosures']= df



    ##########   CHEMICAL RECORDS TABLE   #############
    

    def flag_duplicated_records(self,records):
        records['dup_rec'] = records.duplicated(subset=['UploadKey',
                                                    'IngredientName',
                                                    'CASNumber',
                                                    'MassIngredient',
                                                    'PercentHFJob',
                                                    'PercentHighAdditive'],
                                        keep=False)
        c0 = records.ingKeyPresent
        c1 = records.Supplier.str.lower().isin(['listed above'])
        c2 = records.Purpose.str.lower().str[:9]=='see trade'
        records['dup_rec'] = np.where(records.dup_rec&c0&c1&c2,True,False)
        self.print_step(f'Number dups: {records.dup_rec.sum()}',2)
        return records
    
    def assemble_chem_rec_table(self,raw_df):
        self.print_step('assembling chemical records table')
        df= raw_df[['UploadKey','CASNumber','IngredientName','PercentHFJob',
                    'Supplier','Purpose','TradeName',
                    'PercentHighAdditive','MassIngredient',
                    'ingKeyPresent','reckey','IngredientKey',
                    'density_from_comment']].copy()
        ct.na_check(df,txt='assembling chem_rec 1')
        
        df.Supplier = df.Supplier.fillna('MISSING')

        self.print_step('adding bgCAS',1)
        df = pd.merge(df,self.tables['cas_ing'],
                                on=['CASNumber','IngredientName'],
                                how='left')     
        ct.na_check(df,txt='bgCAS add')
        unCAS = df[df.bgCAS.isna()]\
                    .groupby(['CASNumber','IngredientName'],as_index=False)\
                        ['UploadKey'].count()
        unCAS.columns = ['CASNumber','IngredientName','rec_num']
        if unCAS.rec_num.sum() > 0:
            s = ' <<******'
        else:
            s = ''
        self.print_step(f'Number uncurated CAS/Ingred pairs: {len(unCAS)}, n: {unCAS.rec_num.sum()}{s}',2)
        unCAS.to_csv(self.pkldir+'uncurated_CAS_ING.csv',encoding='utf-8',
                     index=False, quotechar = '$')
        

        self.print_step('create bgSupplier',1)

        cmp = self.tables['companies'][['rawName','xlateName']]
        cmp.columns = ['Supplier','bgSupplier']

        if len(cmp[cmp.Supplier.duplicated()])>0:
            print(f'******  LOOKS like duplicates in COMPANY table: {len(cmp)}')
            # finding duplicates in company field
            print(cmp[cmp.Supplier.duplicated(keep=False)])

        df = pd.merge(df,cmp,on='Supplier',
                                 how='left',validate='m:1')
        ct.na_check(df,txt='bgSupplier add')
        
        unSup = df[df.bgSupplier.isna()]
        if len(unSup)>0: flag = '<******'
        else: flag= ''
        self.print_step(f'Number uncurated Suppliers: {len(unSup)} {flag}',2)

        self.print_step('flagging duplicate records',1)
        self.tables['chemrecs'] = self.flag_duplicated_records(df)
        ct.na_check(df,txt='assembling chem_rec end')

    ############   POST ASSEMBLY PROCESSING   ############

    def flag_empty_disclosures(self):
        self.print_step('flagging disclosures without chem records')
        gb = self.tables['chemrecs'].groupby('UploadKey',as_index=False)['ingKeyPresent'].sum()
        gb['no_chem_recs'] = np.where(gb.ingKeyPresent==0,True,False)
        df = pd.merge(self.tables['disclosures'],
                      gb[['UploadKey','no_chem_recs']],
                      on='UploadKey',how='left')
        self.print_step(f'number empty disclosures: {df.no_chem_recs.sum()} of {len(df)}',1)
        self.tables['disclosures'] = df
        
    def flag_duplicate_disclosures(self):
        self.print_step('flag duplicate disclosures')
        df = self.tables['disclosures'].copy()
        df['api10'] = df.APINumber.str[:10]
        df['dup_disclosures'] = df[~df.no_chem_recs]\
                                  .duplicated(subset=['APINumber',
                                                      'date'],
                                              keep=False)
        df.dup_disclosures = np.where(df.no_chem_recs,False,
                                      df.dup_disclosures)
        bulk_api10 = df[(df.data_source=='bulk')&~(df.no_chem_recs)]\
                       .api10.unique().tolist()


        df['redund_skytruth'] = (df.api10.isin(bulk_api10))&\
                                (df.data_source=='SkyTruth') 
                                
        cond = df.data_source=='SkyTruth'
        df['duplicate_skytruth'] = df[cond].duplicated(subset=['api10','date'],
                                                       keep=False)
        df.duplicate_skytruth = np.where(df.data_source=='bulk',
                                         False,df.duplicate_skytruth)
        
        st_removed_api10 = self.ST_without_pdf.api10.unique().tolist()
        stupk = df[df.api10.isin(st_removed_api10)].UploadKey.unique().tolist()
        df['skytruth_removed'] = df.UploadKey.isin(stupk)
        # removed skytruth disclosures are lumped in the 'is_duplicate' group

        df['is_duplicate'] = df.dup_disclosures | df.redund_skytruth | df.duplicate_skytruth | df.skytruth_removed
        
        upk = df[df.is_duplicate].UploadKey.unique().tolist()

        self.tables['disclosures']['is_duplicate'] = self.tables['disclosures'].UploadKey.isin(upk)
        self.tables['disclosures']['skytruth_removed'] = self.tables['disclosures'].UploadKey.isin(stupk)
        self.print_step(f'n duplicate disclosures within v2 and v3: {df.dup_disclosures.sum()}',1)
        self.print_step(f'n redundant SkyTruth disclosures: {df.redund_skytruth.sum()}',1)
        self.print_step(f'n duplicate SkyTruth disclosures: {df.duplicate_skytruth.sum()}',1)
        self.print_step(f'n SkyTruth disclosures deleted from pdf library: {df.skytruth_removed.sum()}',1)        
        self.print_step(f'n is_duplicate: {df.is_duplicate.sum()}',1)
        
# =============================================================================
#     def apply_auto_record(self,disc,recs,row):
#         disc.loc[row.UploadKey,'carrier_status'] = 'auto'
#         disc.loc[row.UploadKey,'carrier_percent'] = row.PercentHFJob
#         disc.loc[row.UploadKey,'has_water_carrier'] = True
#         recs.loc[row.IngredientKey,'is_water_carrier'] = True 
#         return disc,recs
#     
# =============================================================================
    def apply_carrier_tables(self):
        self.print_step('applying carrier table data')
        ukl = self.tables['disclosures'].UploadKey.unique().tolist()
        #ikl = self.tables['chemrecs'].IngredientKey.unique().tolist()        

        recs = self.tables['chemrecs']
        disc = self.tables['disclosures']
        disc = disc.set_index('UploadKey')
        recs = recs.set_index('IngredientKey')
        
        # set up defaults of new fields
        
        recs['is_water_carrier'] = False
        disc['carrier_status'] = 'unknown'
        #disc['carrier_percent'] = np.NaN
        #disc['has_curated_carrier'] = False
        disc['has_water_carrier'] = False
        disc['carrier_problem_flags'] = ''
        #disc['non_water_carrier'] = False

        # **** auto carrier install
        auto_carrier_df = pd.read_csv(self.trans_dir+'carrier_list_auto.csv',
                                       quotechar = '$',encoding='utf-8',
                                       low_memory=False)
        # pass auto carrier type into disc table
        gb = auto_carrier_df.groupby('UploadKey')['auto_carrier_type'].first()
        disc = pd.merge(disc,gb,on='UploadKey',how='left')

        # to keep sub-runs from failing...
        auto_carrier_df = auto_carrier_df[auto_carrier_df.UploadKey.isin(ukl)]
        self.print_step(f'Auto-detected carriers: {len(auto_carrier_df)}',1)

        # get the auto_carrier label 
        uk = auto_carrier_df.UploadKey.tolist()
        ik = auto_carrier_df.IngredientKey.tolist()
        disc.loc[uk,'has_water_carrier'] = True
        disc.loc[uk,'carrier_status'] = 'auto-detected'
        try:
            recs.loc[ik,'is_water_carrier']  = True
        except:
            print('***ERROR applying is_water_carrier: ignore if this is a test mode')
        
        # **** disclosures with problems preventing carrier detection
        prob_carrier_df = pd.read_csv(self.trans_dir+'carrier_list_prob.csv',
                                       quotechar = '$',encoding='utf-8')
        # to keep sub-runs from failing...
        prob_carrier_df = prob_carrier_df[prob_carrier_df.UploadKey.isin(ukl)]
        self.print_step(f'Problem disclosures excluded: {len(prob_carrier_df)}',1)

        uk = prob_carrier_df.UploadKey.tolist()
        reas = prob_carrier_df.reasons.tolist()
        disc.loc[uk,'carrier_status'] = 'problems-detected; carrier not identified'
        disc.loc[uk,'carrier_problem_flags'] = reas
        
        ### the following steps remove the curation 'helpers' to make it
        ###   a useful file
        mg = pd.read_csv(self.trans_dir+'carrier_list_curated.csv',
                         quotechar='$',encoding='utf-8',low_memory=False,
                         dtype={'is_water_carrier':'str'})
        #print(len(mg))
        # this drops any spacer lines
        mg = mg[mg.IngredientKey.notna()]
        test = ['******','FALSE']
        mg['temp'] = ~mg.is_water_carrier.isin(test)
        mg.is_water_carrier = mg.temp
        gb = mg.groupby('UploadKey',as_index=False)['is_water_carrier'].sum()
        upk = gb[gb.is_water_carrier>0].UploadKey.tolist()
        mg['cur_carrier_status'] = np.where(mg.UploadKey.isin(upk),
                                            'water_based_carrier',
                                            'carrier_not_water')
        
        cur_carrier_df = mg
        
        
        # to keep sub-runs from failing...
        cur_carrier_df = cur_carrier_df[cur_carrier_df.UploadKey.isin(ukl)]
        self.print_step(f'Curation detected carriers: {len(cur_carrier_df[cur_carrier_df.cur_carrier_status=="water_based_carrier"])}',1)

        # first install data from water-based-carriers
        cond = cur_carrier_df.cur_carrier_status=='water_based_carrier'
        uk = cur_carrier_df[cond].UploadKey.tolist()
        disc.loc[uk,'has_water_carrier'] = True
        disc.loc[uk,'carrier_status'] = 'curation-detected'

        ik = cur_carrier_df[cur_carrier_df.is_water_carrier].IngredientKey.tolist()
        recs.loc[ik,'is_water_carrier']  = True
        
        self.tables['disclosures'] = disc.reset_index()
        self.tables['chemrecs'] = recs.reset_index()
        
        

    def make_whole_dataset_flags(self):
        self.print_step('make whole data set flags')
        rec_df, disc_df = mt.prep_datasets(rec_df=self.tables['chemrecs'],
                                           disc_df=self.tables['disclosures'])
        self.tables['chemrecs'] = rec_df
        self.tables['disclosures'] = disc_df

    def mass_calculations(self):
        self.print_step('calculating mass',newlinefirst=True)
        rec_df, disc_df = mt.calc_mass(rec_df=self.tables['chemrecs'],
                                       disc_df=self.tables['disclosures'])
        rec_df = pd.merge(rec_df,disc_df[['UploadKey','within_total_tolerance']],
                          on='UploadKey',how='left')
        rec_df.calcMass = np.where(rec_df.within_total_tolerance,
                                   rec_df.calcMass,np.NaN)
        self.tables['chemrecs'] = rec_df.drop(['within_total_tolerance'],axis=1)
        self.tables['disclosures'] = disc_df
        
        self.print_step(f'number of recs with calculated mass: {len(rec_df[rec_df.calcMass>0]):,}',1)                
        
            
    def gen_primarySupplier(self): 
        self.print_step('generating primarySupplier')
        non_company = ['third party','operator','ambiguous',
                       'company supplied','customer','multiple suppliers',
                       'not a company','missing']
        rec = self.tables['chemrecs'].copy()
        rec = rec[~(rec.bgSupplier.isin(non_company))]
        gb = rec.groupby('UploadKey')['bgSupplier'].agg(lambda x: x.value_counts().index[0])
        gb = gb.reset_index()
        gb.rename({'bgSupplier':'primarySupplier'},axis=1,inplace=True)
        self.tables['disclosures'] = pd.merge(self.tables['disclosures'],
                                              gb,on='UploadKey',how='left',
                                              validate='1:1')
        
    def pickle_tables(self):
        self.print_step('pickling all tables',newlinefirst=True)
        for name in self.tables.keys():
            self.tables[name].to_pickle(self.pickle_fn[name])

            
    def load_pickled_tables(self):
        for t in self.tables.keys():
            self.tables[t] = self.fetch_df(df_name=t)
        
    def show_size(self):
        for name in self.tables.keys():
            self.print_size(self.tables[name],name)
     
    def assemble_all_tables(self,df):
        ct.na_check(df,txt='top of assemble all tables')
        self.assemble_cas_ing_table()
        self.assemble_companies_table()
        self.assemble_bgCAS_table(self.tables['cas_ing'])
        self.assemble_disclosure_table(df)
        self.assemble_chem_rec_table(df)
        self.apply_carrier_tables()
        self.flag_empty_disclosures()
        self.flag_duplicate_disclosures()
        self.gen_primarySupplier()
        self.make_whole_dataset_flags()
        self.mass_calculations()
        self.pickle_tables()
        self.show_size()
        
    def fetch_df(self,df_name='bgCAS',verbose=False):
        df = pd.read_pickle(self.pickle_fn[df_name])
        if verbose:
            print(f'  -- fetching {df_name} df')
        return df
        
    def release_tables(self):
        for name in self.tables:
            self.tables[name] = None
        gc.collect()

    def get_table_creation_date(self):
        try:
            t = os.path.getmtime(self.pickle_fn['chemrecs'])
            return datetime.datetime.fromtimestamp(t)
        except:
            return False

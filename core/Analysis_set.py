# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:22:20 2021

@author: Gary
"""

import core.Table_manager as c_tab
import pandas as pd
import os
import zipfile
import datetime
import core.cas_tools as ct

####### uncomment below for local runs
import common
outdir = common.get_pickle_dir()
sources = common.get_data_dir()
tempfolder = './tmp/'


### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../'



def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

def banner(text):
    print()
    print('*'*80)
    space = ' '*int((80 - len(text))/2)
    print(space,text,space)
    print('*'*80)


class Template_data_set():
    
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 set_name = 'template',
                 pkl_when_creating=True,
                 force_new_creation=False):
        self.set_name= set_name
        self.bulk_fn = bulk_fn
        self.sources = sources
        self.outdir = outdir
        self.df = None
        self.pkldir = self.outdir+self.bulk_fn+'_pickles/'
        self.pkl_fn = self.pkldir+self.set_name+'_df.pkl'
        self.pkl_when_creating = pkl_when_creating
        self.force_new_creation = force_new_creation

        self.t_man = c_tab.Table_constructor(sources = sources,
                                             outdir= outdir,
                                             pkldir=self.pkldir)
        self.table_date = self.t_man.get_table_creation_date()
        if self.table_date==False:
            banner(f"!!! Pickles for data tables don't exist for {self.bulk_fn}.")
            banner('      * Run "build_data_set.py" first *')
            exit()
        self.wC = {}
        self.choose_fields()
        
    def pickle_is_valid(self,verbose=False):
        try:
            df_date = modification_date(self.pkl_fn)
            if verbose:
                print(f'Pickle created: {df_date}')
            return True
        except:
            return False
    
    def get_fn_list(self):
        s = set()
        for t in self.wC.keys():
            for c in self.wC[t]:
                s.add(c)
        return list(s)

    def add_fields_to_keep(self,field_dict = {'bgCAS':['is_on_TEDX']}):
        for table in field_dict.keys():
            for col in field_dict[table]:
                self.wC[table].add(col)
                
    def pickle_set(self):
        self.df.to_pickle(self.pkl_fn)

    def create_set(self):
        self.merge_tables()
        if self.pkl_when_creating:
            self.pickle_set()

    def get_set(self,verbose=False):
        if (self.pickle_is_valid())&~(self.force_new_creation):
            if verbose:
                print('Using saved pickle of analysis set...')
            self.df = pd.read_pickle(self.pkl_fn)
        else:
            if verbose:
                print('Creating data set from scratch...')
            self.t_man.load_pickled_tables()
            self.prep_for_creation()
            self.create_set()
        if verbose:
            print(f'Dataframe ***"{self.set_name}"***\n')
            print(self.df.info())
        return self.df
    
    def prep_for_creation(self):        
        # workTables: point to set of tables to manipulate for current data set
        self.make_work_tables()
        ct.na_check(self.wRec,txt=' prep_for_creation: records')
        ct.na_check(self.wDisc,txt=' prep_for_creation: disclosures')
            
    def save_compressed(self):
        print(f' -- Saving < {self.set_name} > as compressed zip file.')
        self.t_man.load_pickled_tables()
        self.prep_for_creation()
        self.create_set()
        fn = self.set_name
        tmpfn = fn+'.csv'
        self.df.to_csv(tmpfn,index=False) # write in default directory for CodeOcean
        with zipfile.ZipFile(self.outdir+fn+'.zip','w') as z:
             z.write(tmpfn,compress_type=zipfile.ZIP_DEFLATED)
        os.remove(tmpfn)

    def choose_fields(self):
        pass
    def merge_tables(self):
        #print(f'in template merge: {self.wC}')
        pass
    def make_work_tables(self):
        pass

class Standard_data_set(Template_data_set):
    
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 set_name = 'standard_filtered',
                 pkl_when_creating=True,
                 force_new_creation=False):

        Template_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           set_name=set_name,
                           pkl_when_creating=pkl_when_creating,
                           force_new_creation=force_new_creation)

    def make_work_tables(self):
        cond = ~(self.t_man.tables['disclosures'].is_duplicate) &\
               ~(self.t_man.tables['disclosures'].no_chem_recs)
        self.wDisc = self.t_man.tables['disclosures'][cond].copy()
        
        cond = ~(self.t_man.tables['records'].dup_rec)
        self.wRec = self.t_man.tables['records'][cond].copy()
        
        self.wBgCAS = self.t_man.tables['bgCAS'].copy()
        
    def keep_basic_fields(self):
        self.wC['disclosures'] = set(['StateName','CountyName',
                                      'Latitude','Longitude',
                                      'OperatorName','WellName',
                                      'UploadKey','date','APINumber',
                                     'bgStateName','bgCountyName',
                                     'bgLatitude','bgLongitude',
                                     'TotalBaseWaterVolume','TotalBaseNonWaterVolume',
                                     'TVD','bgOperatorName','primarySupplier',
                                     'carrier_status']
                                     )
        self.wC['records'] = set(['UploadKey','CASNumber','IngredientName',
                                  'Supplier','bgCAS','calcMass','categoryCAS',
                                  'PercentHFJob','Purpose','TradeName','bgSupplier',
                                  'is_valid_cas'])
        self.wC['bgCAS'] = set(['bgCAS','bgIngredientName'])
        


    def merge_tables(self):
        #print(f'in std merge: {self.wC}')
        self.df = pd.merge(self.wDisc[self.wC['disclosures']],
                           self.wRec[self.wC['records']],
                           on='UploadKey',
                           how='inner',validate='1:m')
        self.df = pd.merge(self.df,
                           self.wBgCAS[self.wC['bgCAS']],
                           on='bgCAS',
                           how='left',validate='m:1')        

    def choose_fields(self):
        self.keep_basic_fields()
          
class Standard_with_externals(Standard_data_set):
    """like standard_data_set but with external data sets"""
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = True,
                 set_name='std_filtered_with_extrnls',
                 force_new_creation=True):
        Standard_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)        

    def choose_fields(self):
        self.keep_basic_fields()
        self.add_fields_to_keep({'bgCAS':['is_on_TEDX','is_on_prop65',
                                          'we_Pathway',
                                          'is_on_PFAS_list','is_on_CWA','is_on_DWSHA',
                                          'eh_Class_L1','eh_Class_L2']})

class Standard_location(Standard_data_set):
    """like standard_data_set but just location fields"""
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = True,
                 set_name='std_filtered_loc_only',
                 force_new_creation=True):
        Standard_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)        

        
    def merge_tables(self):
        # no merging required; ignore all but disclosure table
        self.df = self.wDisc[self.wC['disclosures']]


class Full_set(Template_data_set):
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = False,
                 set_name='full_no_filter',
                 force_new_creation=True):
        Template_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)
    
    def keep_all_fields(self):
        # for all field, we must have access to all t_man tables, so must load them
        self.t_man.load_pickled_tables()
        self.wC = {}
        for t in self.t_man.tables.keys():
            self.wC[t] = set()
            for fn in list(self.t_man.tables[t].columns):
                self.wC[t].add(fn) 

    def make_work_tables(self):
        self.wDisc = self.t_man.tables['disclosures'].copy()
        self.wRec = self.t_man.tables['records'].copy()       
        self.wBgCAS = self.t_man.tables['bgCAS'].copy()


    def choose_fields(self):
        self.keep_all_fields()

    def merge_tables(self):
        #print(f'in full merge: {self.wC}')
        self.df = pd.merge(self.wDisc,
                           self.wRec,
                           on='UploadKey',
                           how='inner',validate='1:m')
        self.df = pd.merge(self.df,
                           self.wBgCAS,
                           on='bgCAS',
                           how='left',validate='m:1')        
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.no_chem_recs)&\
                                     ~(self.df.dup_rec)

class Catalog_set(Full_set):
    # used to make the catalog, keeps full data, but filter flag too for partitioning
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = False,
                 set_name='catalog_set',
                 force_new_creation=True):
        Full_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)

    def keep_catalog_fields(self):
        self.wC['disclosures'] = set(['StateName','CountyName',
                                      'Latitude','Longitude',
                                      'OperatorName','WellName',
                                      'UploadKey','date','APINumber',
                                     'bgStateName','bgCountyName',
                                     'bgLatitude','bgLongitude',
                                     'TotalBaseWaterVolume','TotalBaseNonWaterVolume',
                                     'TVD','bgOperatorName','primarySupplier',
                                     'is_duplicate','no_chem_recs',
                                     'has_TBWV','has_water_carrier',
                                     'has_curated_carrier',
                                     'within_total_tolerance','data_source',
                                     'carrier_mass','carrier_mass_MI']
                                     )
        self.wC['records'] = set(['UploadKey','CASNumber','IngredientName',
                                  'Supplier','PercentHighAdditive',
                                 'bgCAS','calcMass','categoryCAS',
                                 'PercentHFJob',
                                 'Purpose','TradeName','bgSupplier',
                                 'dup_rec','is_water_carrier','IngredientKey',
                                 'MassIngredient','is_valid_cas'])
        self.wC['bgCAS'] = set(['bgCAS','bgIngredientName','is_on_TEDX',
                                'is_on_prop65','is_on_CWA','is_on_DWSHA',
                                'DTXSID',
                                'is_on_PFAS_list','is_on_volatile_list'])

    def choose_fields(self):
        self.keep_catalog_fields()
        
    def merge_tables(self):
        #print(f'in catalog set: {self.wC}')
        self.df = pd.merge(self.wDisc[self.wC['disclosures']],
                           self.wRec[self.wC['records']],
                           on='UploadKey',
                           how='inner',validate='1:m')
        self.df = pd.merge(self.df,
                           self.wBgCAS[self.wC['bgCAS']],
                           on='bgCAS',
                           how='left',validate='m:1')        
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.no_chem_recs)&\
                                     ~(self.df.dup_rec)



class Full_location(Full_set):
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = False,
                 set_name='full_location_only',
                 force_new_creation=True):
        Full_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)

    
    def merge_tables(self):
        #print(f'in full loc merge: {self.wC}')
        self.df = self.wDisc
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.no_chem_recs)
    
    
class Min_filtered(Standard_data_set):
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = True,
                 set_name='mininal_filtered',
                 force_new_creation=True):
        Standard_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)
    
    def keep_mininal_fields(self):
        self.wC['disclosures'] = set(['UploadKey','date','APINumber'])
        self.wC['records'] = set(['UploadKey','bgCAS','calcMass','PercentHFJob'])

    def choose_fields(self):
        self.keep_mininal_fields()
    def merge_tables(self):
        #print(f'in min filter merge: {self.wC}')
        self.df = pd.merge(self.wDisc[self.wC['disclosures']],
                           self.wRec[self.wC['records']],
                           on='UploadKey',
                           how='inner',validate='1:m')


class Min_filtered_location(Min_filtered):
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = True,
                 set_name='minimal_filtered_loc_only',
                 force_new_creation=True):
        Min_filtered.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)

    def merge_tables(self):
        #print(f'in min filter loc merge: {self.wC}')
        # no merging required; ignore all but disclosure table
        self.df = self.wDisc[self.wC['disclosures']]
    
class Min_no_filter(Standard_data_set):
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = True,
                 set_name='minimal_no_filter',
                 force_new_creation=True):

        Standard_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)
    
    def keep_mininal_fields(self):
        self.wC['disclosures'] = set(['UploadKey','date','APINumber',
                                      'is_duplicate','no_chem_recs'])
        self.wC['records'] = set(['UploadKey','bgCAS','calcMass','PercentHFJob',
                                  'dup_rec'])

    def choose_fields(self):
        self.keep_mininal_fields()

    def make_work_tables(self):
        self.wDisc = self.t_man.tables['disclosures'].copy()
        self.wRec = self.t_man.tables['records'].copy()       

    def merge_tables(self):
        #print(f'in min no filter merge: {self.wC}')
        self.df = pd.merge(self.wDisc[self.wC['disclosures']],
                           self.wRec[self.wC['records']],
                           on='UploadKey',
                           how='inner',validate='1:m')
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.no_chem_recs)&\
                                     ~(self.df.dup_rec)
        self.df = self.df.drop(['is_duplicate','no_chem_recs','dup_rec'],axis=1)

class MI_analysis_set(Full_set):
    # used to make the catalog, keeps full data, but filter flag too for partitioning
    def __init__(self,bulk_fn='currentData',
                 sources=sources,
                 outdir=outdir,
                 pkl_when_creating = False,
                 set_name='MI_analysis_set',
                 force_new_creation=True):
        Full_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name,
                           force_new_creation=force_new_creation)

    def keep_catalog_fields(self):
        self.wC['disclosures'] = set([#'StateName','CountyName',
                                      #'Latitude','Longitude',
                                      'OperatorName',#'WellName',
                                      'UploadKey','date','APINumber',
                                     'bgStateName','bgCountyName',
                                     'bgLatitude','bgLongitude',
                                     'bgDensity','bgDensity_source',
                                     'TotalBaseWaterVolume',#'TotalBaseNonWaterVolume',
                                     'TVD','bgOperatorName','primarySupplier',
                                     'is_duplicate','no_chem_recs',
                                     'has_TBWV','has_water_carrier',
                                     'has_curated_carrier',
                                     'within_total_tolerance','data_source',
                                     'carrier_mass','carrier_mass_MI',
                                     'carrier_density_MI']
                                     )
        self.wC['records'] = set(['UploadKey','CASNumber',#'IngredientName',
                                 'bgCAS','calcMass','category','PercentHFJob',
                                 'Purpose',#'TradeName','bgSupplier',
                                 'dup_rec','is_water_carrier',
                                 'MassIngredient'])
        self.wC['bgCAS'] = set(['bgCAS','bgIngredientName',#'is_on_TEDX',
                                ])

    def choose_fields(self):
        self.keep_catalog_fields()
        
    def merge_tables(self):
        #print(f'in catalog set: {self.wC}')
        self.df = pd.merge(self.wDisc[self.wC['disclosures']],
                           self.wRec[self.wC['records']],
                           on='UploadKey',
                           how='inner',validate='1:m')
        self.df = pd.merge(self.df,
                           self.wBgCAS[self.wC['bgCAS']],
                           on='bgCAS',
                           how='left',validate='m:1')        
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.no_chem_recs)&\
                                     ~(self.df.dup_rec)



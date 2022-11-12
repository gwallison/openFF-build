# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the output data sets.

Change the file handles at the top of this code to appropriate directories.
    
"""
#### -----------   File handles  -------------- ####


####### uncomment below for local runs
import build_common
outdir = build_common.get_pickle_dir()
sources = build_common.get_data_dir()
tempfolder = './tmp/'

#print(outdir, sources)

### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../'

#print(f' outdir: {outdir}')
####### zip input files (local default)
bulk_fn = 'currentData'
stfilename = 'sky_truth_final'

#### ----------    end File Handles ----------  ####

make_files = True # do we create the gigantic output csv/zip files?

import shutil
import os
import core.Bulk_data_reader as rff
import core.Table_manager as c_tab


class Data_set_constructor():
    def __init__(self, bulk_fn=bulk_fn,const_mode='TEST',
                 data_source='bulk',
                 stfilename=stfilename,tempfolder=tempfolder,
                 sources=sources,outdir=outdir,
                 make_files=make_files,
                 startfile=0,endfile=None,
                 abbreviated=False):
        #print(outdir)
        self.outdir = outdir
        self.data_source=data_source
        self.sources = sources
        self.tempfolder = tempfolder
        self.zfilename = self.sources+'bulk_data/'+bulk_fn+'.zip'
        self.stfilename = self.sources+'bulk_data/'+stfilename+'.zip'
        self.make_files=make_files
        self.abbreviated = abbreviated # used to do partial constructions
        self.startfile= startfile
        self.endfile = endfile
        self.const_mode = const_mode
        if const_mode == "TEST": added = '_TEST'
        else: added = ''
        #print(outdir)
        #print(self.outdir)
        #print(f'{self.outdir},{bulk_fn},{added},{"_pickles"}')
        #print(f'{ self.outdir+bulk_fn+added+"_pickles/"}')
        self.picklefolder = self.outdir+bulk_fn+added+'_pickles/'

        
    def initialize_dir(self,dir):
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
                           
    def _banner(self,text):
        print()
        print('*'*50)
        space = ' '*int((50 - len(text))/2)
        print(space,text,space)
        print('*'*50)
        
    def create_full_set(self):
        tab_const = c_tab.Table_constructor(pkldir=self.picklefolder,
                                            outdir = self.outdir,
                                            sources = self.sources,
                                            data_source=self.data_source)
        self.initialize_dir(self.picklefolder)
        self._banner('PROCESS RAW DATA FROM SCRATCH')
        self._banner(f'Reading {self.data_source} Data')
        raw_df = rff.Read_FF(zname=self.zfilename,
                             skytruth_name=self.stfilename,
                             data_source = self.data_source,
                             sources = self.sources,
                             outdir = self.outdir,
                             startfile=self.startfile,
                             endfile=self.endfile).import_all()
        print(f'  -- Number of initial disclosures: {len(raw_df.UploadKey.unique())}')                         
        self._banner('Table_manager')
        mark_missing = ['CASNumber','IngredientName','Supplier','OperatorName']
        for col in mark_missing:
            raw_df[col].fillna('MISSING',inplace=True)
        tab_const.assemble_all_tables(raw_df)
        print(f'  -- Number disclosure in table manager: {len(tab_const.tables["disclosures"])}')
        raw_df = None
        
        
        return tab_const

    def get_full_raw_df(self):
        self._banner('PROCESS RAW DATA FROM SCRATCH')
        self._banner('Reading Bulk Data')
        raw_df = rff.Read_FF(zname=self.zfilename,
                             skytruth_name=self.stfilename,
                             data_source = self.data_source,
                             sources = self.sources,
                             outdir = self.outdir,
                             startfile=self.startfile,
                             endfile=self.endfile).import_all()
        mark_missing = ['CASNumber','IngredientName','Supplier','OperatorName']
        for col in mark_missing:
            raw_df[col].fillna('MISSING',inplace=True)
        #print(f'in Data_set:\n{raw_df.columns}')
        return raw_df
        


    def create_quick_set(self):
        """ generates a set of mostly of raw values - used mostly
        in pre-process screening. """
        
        tab_const = c_tab.Construct_tables(pkldir=self.picklefolder,
                                           data_source=self.data_source)
        raw_df = rff.Read_FF(zname=self.zfilename,
                             startfile=self.startfile,
                             data_source=self.data_source,
                             endfile=self.endfile).import_all()
        return tab_const

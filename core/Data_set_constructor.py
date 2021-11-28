# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the output data sets.

Change the file handles at the top of this code to appropriate directories.
    
"""
#### -----------   File handles  -------------- ####

# =============================================================================
# ####### uncomment below for local runs
# outdir = './out/'
# sources = './sources/'
# tempfolder = './tmp/'
# =============================================================================

####### uncomment below for local runs
import common
outdir = common.get_pickle_dir()
sources = common.get_data_dir()
tempfolder = './tmp/'



### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../'


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
                 stfilename=stfilename,tempfolder=tempfolder,
                 sources=sources,outdir=outdir,
                 make_files=make_files,
                 startfile=0,endfile=None,
                 abbreviated=False):

        self.outdir = outdir
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
        
    def create_full_set(self,inc_skyTruth=True):
        tab_const = c_tab.Table_constructor(pkldir=self.picklefolder,
                                            outdir = self.outdir,
                                            sources = self.sources)
        self.initialize_dir(self.picklefolder)
        self._banner('PROCESS RAW DATA FROM SCRATCH')
        self._banner('Reading Bulk Data')
        raw_df = rff.Read_FF(zname=self.zfilename,
                             skytruth_name=self.stfilename,
                             sources = self.sources,
                             outdir = self.outdir,
                             startfile=self.startfile,
                             endfile=self.endfile).\
                                  import_all(inc_skyTruth=inc_skyTruth)
        self._banner('Table_manager')
        mark_missing = ['CASNumber','IngredientName','Supplier','OperatorName']
        for col in mark_missing:
            raw_df[col].fillna('MISSING',inplace=True)
        tab_const.assemble_all_tables(raw_df)
        raw_df = None
        
        
        return tab_const

    def get_full_raw_df(self,inc_skyTruth=True):
        #tab_const = c_tab.Table_constructor(pkldir=self.picklefolder)
        #self.initialize_dir(self.picklefolder)
        self._banner('PROCESS RAW DATA FROM SCRATCH')
        self._banner('Reading Bulk Data')
        raw_df = rff.Read_FF(zname=self.zfilename,
                             skytruth_name=self.stfilename,
                             sources = self.sources,
                             outdir = self.outdir,
                             startfile=self.startfile,
                             endfile=self.endfile).\
                                  import_all(inc_skyTruth=inc_skyTruth)
        #self._banner('Table_manager')
        mark_missing = ['CASNumber','IngredientName','Supplier','OperatorName']
        for col in mark_missing:
            raw_df[col].fillna('MISSING',inplace=True)
        return raw_df
        


    def create_quick_set(self,inc_skyTruth=True):
        """ generates a set of mostly of raw values - used mostly
        in pre-process screening. """
        
        tab_const = c_tab.Construct_tables(pkldir=self.picklefolder)
        raw_df = rff.Read_FF(zname=self.zfilename,
                             startfile=self.startfile,
                             endfile=self.endfile,
                             skytruth_name=self.stfilename).import_all(inc_skyTruth=inc_skyTruth)
# =============================================================================
#         raw_df = tab_const.add_indexes_to_full(raw_df)
#         tab_const.build_tables(raw_df)
#         raw_df = None
# =============================================================================
        return tab_const

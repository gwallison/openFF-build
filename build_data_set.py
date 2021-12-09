# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the tables used to make data sets.

Change the file handles at the top of core.Data_set_constructor to point to appropriate
directories.

    
"""

bulk_fn = 'currentData'
construct_from_scratch = False
do_end_tests = True
make_output_files = False
do_abbrev = False

startfile = 0 # 0 for full set
endfile = None  # None for no upper limit
inc_skyTruth = True
#inc_skyTruth = False

if (startfile!=0) | (endfile!=None) | (inc_skyTruth==False):
    # test mode does not overwrite production mode pickles
    mode = 'TEST'
    print('\n'+30*'-'+ 'Performing in TEST mode!'+30*'-'+'\n')
else:
    mode = 'PRODUCTION'

import core.Data_set_constructor as set_const
import core.Analysis_set as ana_set

if construct_from_scratch:
    # this can be skipped when testing if pickles already made
    t = set_const.Data_set_constructor(bulk_fn=bulk_fn, const_mode=mode,
                                       make_files=make_output_files,
                                       startfile=startfile,
                                       endfile=endfile,
                                       abbreviated=do_abbrev)\
                 .create_full_set(inc_skyTruth=inc_skyTruth)

if do_end_tests:
    import core.Tests_of_final as tests
    print('\nStarting tests of final product')
    print('  --  Creating test data set')
    df = ana_set.Full_set(bulk_fn=bulk_fn,
                          pkl_when_creating=False).get_set()
    tests.run_all_tests(df)

if make_output_files == True:
    print('\n\n -- Generating output data sets\n')

    ana_set.Standard_data_set(bulk_fn=bulk_fn,
                              pkl_when_creating=False).save_compressed()

    ana_set.Full_set(bulk_fn=bulk_fn,
                              pkl_when_creating=False).save_compressed()

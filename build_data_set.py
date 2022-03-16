# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the tables used to make data sets.

Change the file handles at the top of core.Data_set_constructor to point to appropriate
directories.

Within Open-FF, there are three "working" FracFocus raw files.  When new data is
downloaded from FracFocus, we put it in a file named "test_data.zip" (If we
are performing a tripwire scan, we move the previous "test_data.zip" into 
"test_data_last.zip"").  These two files are handled in the script: 
"get_new_raw_file.py" and is performed frequently to document new disclosures.
That script also saves a copy of test_data.zip into an archive directory once
a week - usually the same day that the weekly blog post is generated.

The third working raw file in Open-FF is "curentData.zip".  This is simply a
copy of "test_data.zip" made once I'm ready to start a curation process.  This
separation of files allows Open-FF to continue to download data into test_data.zip 
while I am working on the curation and repository process (which can take several
days).  The currentData.zip file is typically what is used in this script to 
build a file eventually ready for a repository.

# 3/2022 - removing Skytruth archive
    
"""

bulk_fn = 'currentData'        # name of raw archive file
construct_from_scratch = True  # normally True
do_end_tests = True            # normally True
make_output_files = False      # True for final runs, adds lots of compile time
do_abbrev = False              # normally False, for some testing purposes

startfile =  0 # 0 for full set
endfile = None  # None for no upper limit

if (startfile!=0) | (endfile!=None) :
    # test mode does not overwrite production mode pickles
    mode = 'TEST'
    print('\n'+30*'-'+ 'Performing in TEST mode!'+30*'-'+'\n')
else:
    mode = 'PRODUCTION'

import core.Data_set_constructor as set_const
import core.Analysis_set as ana_set


def run_build(bulk_fn = bulk_fn,
              mode=mode,
              make_output_files=make_output_files,
              startfile=startfile,
              endfile=endfile,
              do_abbrev=do_abbrev,
              do_end_tests=do_end_tests,
              construct_from_scratch=construct_from_scratch):
    
    if construct_from_scratch:
        # this can be skipped when testing if pickles already made
        t = set_const.Data_set_constructor(bulk_fn=bulk_fn, const_mode=mode,
                                           make_files=make_output_files,
                                           startfile=startfile,
                                           endfile=endfile,
                                           abbreviated=do_abbrev)\
                     .create_full_set()
    if do_end_tests&(mode=='PRODUCTION'):
        import core.Tests_of_final as tests
        print('\nStarting tests of final product')
        print('   Creating test set of FULL data')
        df = ana_set.Full_set(bulk_fn=bulk_fn,
                              pkl_when_creating=False).get_set()
        tests.final_test(df).run_all_tests()
    
    if make_output_files == True:
        print('\n\n -- Generating output data sets\n')
    
        ana_set.Standard_data_set(bulk_fn=bulk_fn,
                                  pkl_when_creating=False).save_compressed()    
        ana_set.Full_set(bulk_fn=bulk_fn,
                                  pkl_when_creating=False).save_compressed()
    
    print('\nBuild completed\n')
    try:
        return t
    except:
        print('No data set constructor to return.')


if __name__ == '__main__':
    t = run_build() # build using the defaults at the top.
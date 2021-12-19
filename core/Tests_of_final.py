# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 08:25:23 2021

@author: Gary

These routines are used to check the products of a build to verify some
fundamental characteristics of the final data sets.
"""
#import pandas as pd


class final_test():
    def __init__(self,df=None):
        self.df = df
        
    def print_stage(self,txt):
        print(f'  --  {txt}')
        
    
    def reckey_test(self):
        """the reckey field provides a unique incrementing id for ALL records. The
        basic relationship should be that the number of total records is the same
        as the number of unique reckeys.  Also the max reckey should be 1 less than
        the length of the df (python starts counting at zero)."""
        self.print_stage('Test <reckey> consistency')
        assert len(self.df.reckey.unique())==(len(self.df)),'reckey error in Full'
        assert len(self.df.reckey.unique())==(self.df.reckey.max()+1),'biggest reckey doesn"t match number of unique'

    def bgCAS_test(self):
        """ assuring that bgCAS has been assigned to all records"""
        self.print_stage('Test <bgCAS> consistency')
        assert self.df.bgCAS.isna().sum() == 0, 'bgCAS records with NaN'
        
    def duplicate_test(self):
        """confirms that no records marked as duplicates (either at the disclosure
        or chemrec level) are included in the std filtered data."""
        self.print_stage('Confirm removal of duplicate records from filtered data')
        assert self.df[self.df.in_std_filtered].dup_rec.sum()==0,'dup_rec==True records in filtered'
        assert self.df[self.df.in_std_filtered].is_duplicate.sum()==0, 'is_duplicate==True records in filtered'
        assert self.df[self.df.in_std_filtered].skytruth_removed.sum()==0, 'skytruth_removed==True records in filtered'
        
    def APINumber_test(self):
        """confirms that all records have APINumbers, that the APINumbers are
        consistent with the StateNumber and CountyNumber, that they are
        strings (not integers) to maintain leading zeros, and they are all
        14 characters long."""
        self.print_stage('Confirm APINumber integrity')
        assert self.df.APINumber.isna().sum()==0, 'There are some NaN in APINumber'
        assert self.df.APINumber.dtype=='O', f'APINumber should be dtype "O", but is {self.df.APINumber.dtype}'
        self.df['apilen'] = self.df.APINumber.str.len()
        assert  self.df.apilen.max()==14, f'APINumber length max=={self.df.apilen.max()}'        
        assert  self.df.apilen.min()==14, f'APINumber length min=={self.df.apilen.min()}'
        
        assert self.df.StateNumber.dtype=='int64', f'StateNumber be dtype "int64", but is {self.df.StateNumber.dtype}'
        assert self.df.CountyNumber.dtype=='int64', f'CountyNumber be dtype "int64", but is {self.df.CountyNumber.dtype}'
        
        assert  (self.df.APINumber.str[:2].astype('int')==self.df.StateNumber).all(), 'APINumber[:2] do not match StateNumber'
        assert  (self.df.APINumber.str[2:5].astype('int')==self.df.CountyNumber).all(), 'APINumber[2:5] do not match CountyNumber'
        
    def run_all_tests(self):
        self.reckey_test()
        self.bgCAS_test()
        self.duplicate_test()
        self.APINumber_test()
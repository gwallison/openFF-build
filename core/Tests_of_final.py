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
        self.print_stage('Testing <reckey> consistency')
        assert len(self.df.reckey.unique())==(len(self.df)),'reckey error in Full'
        assert len(self.df.reckey.unique())==(self.df.reckey.max()+1),'biggest reckey doesn"t match number of unique'

    def bgCAS_test(self):
        """ assuring that bgCAS has been assigned to all records"""
        self.print_stage('Testing <bgCAS> consistency')
        assert self.df.bgCAS.isna().sum() == 0, 'bgCAS records with NaN'
        
    def run_all_tests(self):
        self.reckey_test()
        self.bgCAS_test()
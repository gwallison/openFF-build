# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 08:25:23 2021

@author: Gary

These routines are used to check the products of a build to verify some
fundamental characteristics of the final data sets.
"""
#import pandas as pd

def fetch_full_from_zip():
    return None

def print_stage(txt):
    print(f'  --  {txt}')

def reckey_test(df):
    """the reckey field provides a unique incrementing id for ALL records. The
    basic relationship should be that the number of total records is the same
    as the number of unique reckeys.  Also the max reckey should be 1 less than
    the length of the df (python starts counting at zero)."""
    print_stage('Testing <reckey> consistency')
    assert len(df.reckey.unique())==len(df),'reckey error'

def run_all_tests(df):
    reckey_test(df)


if __name__ == '__main__':
    df = fetch_full_from_zip()
    run_all_tests(df)
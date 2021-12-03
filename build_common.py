# -*- coding: utf-8 -*-
"""
Created on Sat Nov 27 21:42:19 2021

@author: Gary
"""

import os


def get_top():
    p = os.getcwd()
    # get grandparent dir
    return os.path.dirname(os.path.dirname(p))

def get_data_dir():
    return get_top() + '/data/'

def get_pickle_dir():
    return get_top() + '/data/pickles/'

def get_transformed_dir():
    return get_top() + '/data/transformed/'

def get_repo_dir():
    return get_top() + '/data/repos/'
    
#print(get_data_dir())
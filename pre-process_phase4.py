# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

"""
import pandas as pd
#import numpy as np
#import gc
import builder_tasks.Carrier_1_identify_in_new as car1

data_source = 'bulk'

q = input(f'Data source is set to <{data_source}>.  Enter "y" to continue.  > ')

if q == 'y':
    # fetch the raw df pickle made in pre-process_1
    df = pd.read_pickle('./tmp/carrier_df.pkl')
    
    car_obj = car1.Carrier_ID(df,data_source=data_source)



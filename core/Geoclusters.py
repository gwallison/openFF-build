# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 16:52:03 2020

@author: Gary

"""

import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import geodesic

import common
trans_dir = common.get_transformed_dir()

class Geoclusters():
    
    def __init__(self, locdf = None):  
        self.df = locdf.copy()
             


    def find_max_distance(self,df,centroids):
        # returns spread: the largest distance between a point and 
        #   the centroid
        # note this uses an approximation - not the always the single correct
        # answer.
        centdic = {}
        for i,row in centroids.iterrows():
            centdic[row.clusterID] = (row.centroidLat,row.centroidLon)
        
        dic = {}
        gb = df.groupby('clusterID',as_index=False)['UploadKey'].count()
        print(f'max disclosure in a cluster: {gb.UploadKey.max()}')
        # for clusters with only one
        for clus in gb[gb.UploadKey<=1].clusterID.unique().tolist():
            dic[clus] = 0
        print(f'    -- number of single disclosure clusters: {len(gb[gb.UploadKey==1])}')
        for idx,clus in enumerate(gb[gb.UploadKey>1].clusterID.unique().tolist()):
            coords = df[df.clusterID==clus][['bgLatitude','bgLongitude']]  
            coords['dup'] = coords.duplicated(subset=['bgLatitude','bgLongitude'])
            if len(coords[~coords.dup])<=1:
                dic[clus] = 0
            else: # need to get max of all different pairs
                diffmax = 0
                comp = coords[~coords.dup]
                try:
                    for icnt,row in comp.iterrows():
                        dist = geodesic(centdic[clus],
                                        (row.bgLatitude,row.bgLongitude)).feet
                        if dist > diffmax:
                            diffmax = dist
                except:
                    diffmax = -9999
                    #print(f'geodesic exception in cluster {clus}')

                dic[clus] = diffmax
            if idx%1000==0: print(idx)
            #if idx>3000:
            #    break
        out = pd.DataFrame(pd.Series(dic)).reset_index()
        out.columns = ['clusterID','spread']
        #print(f'Spread df: {out.head}')
        return out


    def make_simple_clusters(self,eps=0.2):
        coords = self.df[['bgLatitude','bgLongitude']].values
        kms_per_radian = 6371.0088
        epsilon = eps / kms_per_radian
        print('Starting simple clusters...')
        db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
        self.df['clusterID'] = db.labels_.astype('str')
        self.df.clusterID = 'c'+self.df.clusterID.str.zfill(6)
        print(f'  -- {len(self.df.clusterID.unique())} found')
        
        #clusters = self.df.clusterID.unique().tolist()
        print('  -- calculating centroids')
        centroids = self.df.groupby('clusterID',as_index=False)[['bgLatitude','bgLongitude']].mean()
        centroids = centroids.rename({'bgLatitude':'centroidLat',
                                      'bgLongitude':'centroidLon'},
                                     axis=1)
        print('  -- calculating spread')                                                         
        spreads = self.find_max_distance(self.df,centroids)      
        #print(f'spread max: {spreads.spread.max()}')
        clusdf = pd.merge(spreads,centroids,on='clusterID',how='left')
        clusdf.to_csv(trans_dir+'clusters.csv',index=False)
        return self.df



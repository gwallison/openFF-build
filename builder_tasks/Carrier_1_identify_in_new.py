# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 15:37:55 2021

@author: Gary
"""
import pandas as pd
import numpy as np
import build_common
trans_dir = build_common.get_transformed_dir()


lower_tolerance = 95
upper_tolerance = 105

density_min = 6.0
density_max = 13.0

# Normally set to True
remove_dropped_keys = True

class Carrier_ID():
    def __init__(self,input_df,data_source='bulk'):
        self.remove_dropped_keys = remove_dropped_keys
        if not self.remove_dropped_keys:
            print('  -- Not removing dropped keys from carrier sets')
        self.df = input_df
        self.in_upk = self.df.UploadKey
        self.in_ik = self.df.IngredientKey
        self.data_source = data_source
        self.auto_fn = trans_dir+f'{data_source}/carrier_list_auto.csv'
        self.curdf_fn = trans_dir+f'{data_source}/carrier_list_curated.csv'
        self.probdf_fn = trans_dir+f'{data_source}/carrier_list_prob.csv'
        
        # list of single purpose lables for carriers
        self.wlst = ['carrier / base fluid', 'carrier/base fluid', 'carrier fluid',
                'carrier','base fluid','base carrier fluid','carrier/base flud',
                'base fluid / carrier','carrier/ base fluid','base/carrier fluid',
                'carrier base fluid','water','base fluid ',' carrier / base fluid ',
                'base fluid & mix water', 'base fluid & mix water,', 'fresh water',
                'carrier/base fluid ', 'treatment carrier', 'carrier/basefluid',
                'carrying agent', 'base / carrier fluid', 'carrier / base fluid - water',
                'carrier fluid ', 'base frac fluid', 'water',
                'water / produced water', 'carrier ', 'base carrier',
                'fracture fluid', 'frac base fluid']
        self.proppants = ['14808-60-7','1302-93-8','1318-16-7','1302-74-5','1344-28-1','14464-46-1','7631-86-9','1302-76-7']
        self.gasses = ['7727-37-9','124-38-9']
        self.merge_bgCAS()
        self.make_MI_fields()
        self.make_percent_sums()
        self.fetch_carrier_lists()
        self.check_for_prob_disc()
        self.check_for_auto_disc()
        self.check_auto_against_list()
        self.save_curation_candidates()
        
    
    def check_for_removed_keys(self,ref_df,do_IngKey=True):
        """When saved IngredientKeys are missing in new data sets, we drop the
        associated disclosures from the curated list.  This forces a new evaluation
        of those disclosures in case they have been changed."""
        if self.remove_dropped_keys:
            testupk = pd.merge(self.in_upk,ref_df[['UploadKey']],
                               on='UploadKey',how='outer',indicator=True)
            #print(testupk[testupk['_merge']=='right_only'])
            dropkeys = testupk[testupk['_merge']=='right_only'].UploadKey.tolist()
            if len(dropkeys)>0:
                print(f'       ** Dropping {len(dropkeys)} carriers because UploadKeys are missing in latest data')
                ref_df = ref_df[~(ref_df.UploadKey.isin(dropkeys))]
            #print(testupk.head(10))
            if do_IngKey:
                testik = pd.merge(self.in_ik,ref_df[['IngredientKey']],
                                   on='IngredientKey',how='outer',indicator=True)
                #print(testik[testik['_merge']=='right_only'])
                dropkeys = testik[testik['_merge']=='right_only'].IngredientKey.tolist()
                if len(dropkeys)>0:
                    print(f'      ** Dropping {len(dropkeys)} carriers because IngredientKeys are missing in latest data')
                    ref_df = ref_df[~(ref_df.IngredientKey.isin(dropkeys))]
        return ref_df
    
    def fetch_carrier_lists(self):
        print('  -- loading auto-detected records')
        self.autodf = pd.read_csv(self.auto_fn,low_memory=False,
                                  quotechar='$',encoding='utf-8')
        self.autodf['is_new'] = False
        self.autodf = self.check_for_removed_keys(self.autodf)        
        self.remove_disclosures(self.autodf)
        
        print('  -- loading curation-detected records')
        self.curdf = pd.read_csv(self.curdf_fn,low_memory=False,
                                  quotechar='$',encoding='utf-8')
        self.curdf['is_new'] = False
        self.curdf = self.check_for_removed_keys(self.curdf)        
        self.remove_disclosures(self.curdf)

        print('  -- loading problem records')
        self.probdf = pd.read_csv(self.probdf_fn,low_memory=False,
                                  quotechar='$',
                                  encoding='utf-8')
        self.probdf['is_new'] = False
        self.probdf = self.check_for_removed_keys(self.probdf,do_IngKey=False)        
        self.remove_disclosures(self.probdf)
    

    def merge_bgCAS(self):
        #casing =  pd.read_csv('./sources/casing_curate_master.csv',
        casing =  pd.read_csv(trans_dir+'casing_curated.csv',
                              quotechar='$',encoding='utf-8')
        casing['is_valid_CAS'] = casing.bgCAS.str[0].isin(['0','1','2','3','4',
                                                           '5','6','7','8','9'])
        self.df = pd.merge(self.df,casing[['CASNumber','IngredientName',
                                           'bgCAS','is_valid_CAS']],
                           on=['CASNumber','IngredientName'],how='left')
        self.df.is_valid_CAS.fillna(False,inplace=True)
        
    def make_MI_fields(self):
        # remove records that are more likely unreliable: when MI is small
        cond = (self.df.MassIngredient>2)&(self.df.PercentHFJob>0)
        t = self.df[cond][['MassIngredient','PercentHFJob','UploadKey']].copy()
        
        # make a simple ratio of MI to %HFJ.  If everything is consistent, this
        # ratio should essentially be the same for all records in a disclosure
        t['permassratio'] = t.MassIngredient/t.PercentHFJob
        gb = t.groupby('UploadKey',as_index=False)['permassratio'].agg(['min','max']).reset_index()
        gb.columns = ['UploadKey','small','big']
        gb['rat_dev'] = (gb.big-gb.small)/gb.big
        # set MIok to true if the range within a disclosure is less than 10%
        #    MIok is a disclosure level flag.
        gb['MIok'] = gb.rat_dev<.1
        
        print(f'Creating MIok: Number disclosures with MI: {len(gb)}, out of tolerance: {len(gb[gb.rat_dev>0.1])}')

        self.df = pd.merge(self.df,gb[['UploadKey','MIok']],on='UploadKey',how='left')
        self.df.MIok = np.where(~cond,False,self.df.MIok)  
        
        cond2 = (self.df.MassIngredient>5)&(self.df.TotalBaseWaterVolume>10000)&self.df.MIok
        self.df['dens_test'] = np.where(cond2,
                               self.df.MassIngredient/self.df.TotalBaseWaterVolume,
                               np.NaN)
        # density can be within pretty wide range; will check again at 
        c1 = self.df.dens_test>density_min
        c2 = self.df.dens_test<density_max
        self.df['maybe_water_by_MI']=np.where(c1&c2,'yes','no')
        self.df.maybe_water_by_MI = np.where(self.df.dens_test.isna(),
                                             'not testable',
                                             self.df.maybe_water_by_MI)        
        
    def make_percent_sums(self):
        gball = self.df.groupby('UploadKey',as_index=False)[['PercentHFJob',
                                                            'is_valid_CAS']].sum()
        gball['has_no_percHF'] = ~(gball.PercentHFJob>0)
        gball['has_no_valid_CAS'] = ~(gball.is_valid_CAS>0)

        gbmax = self.df.groupby('UploadKey',as_index=False)[['PercentHFJob',
                                                             'TotalBaseWaterVolume']].max()
        gbmax.columns = ['UploadKey','PercMax','TBWV']
        gball = pd.merge(gball,gbmax,on='UploadKey',how='left')

        cond = self.df.PercentHFJob>0
        gbw = self.df[cond].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gbw.columns = ['UploadKey','percSumAll']
        gbwo = self.df[cond&self.df.is_valid_CAS].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gbwo.columns = ['UploadKey','percSumValid']
        gbwoSA = self.df[cond&(~(self.df.bgCAS=='sysAppMeta'))].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gbwoSA.columns = ['UploadKey','percNoSysApp']
        mg = pd.merge(gball,gbw,on=['UploadKey'],how='left')
        mg = pd.merge(mg,gbwo,on='UploadKey',how='left')
        mg = pd.merge(mg,gbwoSA,on='UploadKey',how='left')
        c1 = self.df.bgCAS.isin(self.proppants)
        c2 = self.df.Purpose == 'Proppant'
        gbprop = self.df[cond&(c1|c2)].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gbprop.columns = ['UploadKey','percProp']
        mg = pd.merge(mg,gbprop,on='UploadKey',how='left')
        gbgas = self.df[self.df.bgCAS.isin(self.gasses)].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gbgas.columns = ['UploadKey','percGas']
        self.disc = pd.merge(mg,gbgas,on='UploadKey',how='left')
        
    def addToProbDict(self,dic,UploadKeyList,problem):
        for upl in UploadKeyList:
            dic.setdefault(upl, []).append(problem)
        return dic            

    def check_for_prob_disc(self):
        d = {}
        
        upkl = self.disc[~(self.disc.TBWV>0)].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 1)

        upkl = self.disc[self.disc.percSumValid>upper_tolerance].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 3)

        upkl = self.disc[self.disc.percNoSysApp>upper_tolerance].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 4)

        upkl = self.disc[self.disc.has_no_percHF].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 2)
        
        upkl = self.disc[self.disc.has_no_valid_CAS].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 0)
        
        upkl = self.disc[self.disc.percProp>=50].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 5)

        upkl = self.disc[self.disc.percSumAll<90].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 6)

        # if gasses are dominant        
        upkl = self.disc[self.disc.percGas>=50].UploadKey.unique().tolist()
        d = self.addToProbDict(d, upkl, 7)
        
# =============================================================================
#         # if MI not ok - when something is wrong with MI, can't trust other numbers.
#         upkl = self.disc[~self.disc.MI_inconsistent].UploadKey.unique().tolist()
#         d = self.addToProbDict(d, upkl, 8)
#         
# =============================================================================
# =============================================================================
#         cond = (self.disc.percSumValid>0.95) & (self.disc.percSumValid<1.05)
#         upkl = self.disc[cond].UploadKey.unique().tolist()
#         d = self.addToProbDict(d, upkl, 5)
# =============================================================================
        
        print(f'New problem disclosures found: {len(d)} ')
        uploadKeys = []
        problems = []
        for upk in d.keys():
            uploadKeys.append(upk)
            problems.append(str(d[upk])[1:-1])
        pdf = pd.DataFrame({'UploadKey':uploadKeys,
                            'reasons':problems})
        pdf['is_new'] = True
        self.probdf = pd.concat([pdf,self.probdf],sort=True)
        self.probdf.to_csv('./tmp/carrier_list_prob_NEW.csv',encoding= 'utf-8',quotechar='$',index=False)

        self.remove_disclosures(pdf)
        
    def auto_set_1(self):
        """ THis is the most basic auto algorithm set:
            - looking only at records with valid CAS numbers
            - single record with a carrier purpose
            - CASNumber is water
            - 50% < %HFJob < 100% (single 100% records not ok) 
        """
        
        t = self.df[self.df.is_valid_CAS].copy()
        t['has_purp'] = t.Purpose.str.strip().str.lower().isin(self.wlst)
        gbp = t.groupby('UploadKey',as_index=False)['has_purp'].sum()
        t = t.drop('has_purp',axis=1)
        t = pd.merge(t,gbp,on='UploadKey',how='left')
        
        #print(f'IS IN t: {(t.UploadKey=="ffd52c1a-1868-4b7f-a8a8-47c6621d4802").sum()}')
    
        c1 = t.has_purp==1  # only 1 record with Purpose in wlst
        c2 = t.bgCAS == '7732-18-5'  # must be water
        c3 = (t.PercentHFJob >= 50)&(t.PercentHFJob < 100)  # should be at least this amount
        c4 =  t.Purpose.str.strip().str.lower().isin(self.wlst)
        slic = t[c1&c2&c3&c4][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's1'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_1: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_2(self):
        """ THis basic auto algorithm set allows more than one water record, but still restricted:
            - only include records with valid CAS numbers as water
            - sum of %HFJob for all is < 100 
            - sum of water records should be >50 % (otherwise we pick up Nitrogen dominated fracks)
            Note this can still produce single record carriers if only one of 
            the identified 'carrier/base' records meets the criteria especially
            that there is more than one carrier record, but only one is water.  Set 1
            requires that there is only ONE carrier record.
        """
        
        t = self.df[self.df.is_valid_CAS].copy()
        #print(self.df.columns)
        t['has_purp'] = (t.Purpose.str.strip().str.lower().isin(self.wlst))\
                        &(t.PercentHFJob>0)  # prevent some carriers with no %HFJ from the calculation
                                             # Added 11/9/2021, after removing all previous S2 from 
        gbp = t.groupby('UploadKey',as_index=False)['has_purp'].sum() 
        gbwater = t[t.bgCAS=='7732-18-5'].groupby('UploadKey',as_index=False)\
            ['PercentHFJob'].sum().rename({'PercentHFJob':'perc_water'},axis=1)
        t = t.drop('has_purp',axis=1)
        t = pd.merge(t,gbp,on='UploadKey',how='left')
        t = pd.merge(t,gbwater,on='UploadKey',how='left')
        
        # first find each prospective record could be part of carrier
        c1 = t.has_purp>1  # requires more than one carrier in disclosure
        c2 = t.bgCAS == '7732-18-5'  # keep only water records as carrier
        c3 = t.Purpose.str.strip().str.lower().isin(self.wlst) 
        c4 = t.PercentHFJob > 0  # added 11/9/2021
        c5 = t.perc_water>=50 # added 11/15/2021
        slic = t[c1&c2&c3&c4&c5][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume','MIok']].copy()

        # make sure sum percentage of slic records is not too much
        gb = slic.groupby('UploadKey',as_index=False)[['PercentHFJob']].sum()
        gb['test'] = gb.PercentHFJob<100
        #print(f'Auto_set_2: detected length {len(gb)} ')
        slic = pd.merge(slic,gb[['UploadKey','test']],on='UploadKey',how='left')
        slic = slic[slic.test]
        slic = slic[slic.test].drop('test',axis=1)
                        
        # check what MI has to say about these fields
        gb = slic.groupby('UploadKey',as_index=False)[['MassIngredient']].sum()
        gb.columns = ['UploadKey','sumMass']
        gb2 = slic.groupby('UploadKey',as_index=False)[['TotalBaseWaterVolume','MIok']].first()
        gb = pd.merge(gb,gb2,on='UploadKey',how='left')
        gb['dens_test'] = gb.sumMass/gb.TotalBaseWaterVolume
        gb['no_keep'] = ((gb.dens_test<density_min)|(gb.dens_test>density_max))&gb.MIok
        print(f"Auto_set_2: new {len(gb)}, not kept (density out of range): {len(gb[gb.no_keep])}")
        slic = pd.merge(slic,gb[['UploadKey','no_keep']],on='UploadKey',how='left')
        slic = slic[~slic.no_keep].drop('no_keep',axis=1)
        
        checkver = pd.concat([slic,pd.DataFrame({'UploadKey':slic.UploadKey.unique().tolist()})])
        checkver.sort_values(['UploadKey','PercentHFJob'],ascending=False).to_csv('./tmp/temp.csv')

        slic['auto_carrier_type'] = 's2'
        slic['is_new'] = True
        return slic

    def auto_set_3(self):
        """ Set3 has three conditions:
            - CASNumber is water (7732-18-5)
            - IngredientName has the words "including mix water" (a common identifier)
            - that record is > 40% PercentHFJob 
            
            These records do not have direct indications of carrier records in
            the Purpose (which is often cluttered with multiple purposes) but
            are clearly single record water-based carriers.
        """
        
        t = self.df[self.df.is_valid_CAS].copy()
        c1 = t.IngredientName.str.contains('including mix water')
        c2 = t.bgCAS == '7732-18-5'  # must be water
        c3 = (t.PercentHFJob >= 40)&(t.PercentHFJob < 100)  # should be at least this amount
        slic = t[c1&c2&c3][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's3'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_3: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (density out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_4(self):
        """ Set4 has four conditions:
            - CASNumber is 'MISSING'
            - IngredientName has the words "including mix water" (a common identifier)
            - that record is > 60% PercentHFJob
            - the total_percent_valid_job (including the "including mix" record) is <105%
            
            These records do not have direct indications of carrier records in
            the Purpose (which is often cluttered with multiple purposes) but
            are clearly single record water-based carriers.
        """
        precond = (self.df.CASNumber=='MISSING')&\
                (self.df.IngredientName.str.contains('including mix water'))&\
                ((self.df.PercentHFJob >= 60)&(self.df.PercentHFJob < 100))
        #print(f'Number of raw records with primary condition: {precond.sum()}')
        t = self.df[(self.df.is_valid_CAS)|precond|(self.df.bgCAS=='proprietary')].copy()
        gb = t.groupby('UploadKey',as_index=False)['PercentHFJob'].sum()\
            .rename({'PercentHFJob':'totPercent'},axis=1)
        t = pd.merge(t,gb,on='UploadKey',how='left')
        # calc total%
        cond = (t.CASNumber=='MISSING')&\
                (t.IngredientName.str.contains('including mix water'))&\
                ((t.PercentHFJob >= 60)&(t.PercentHFJob < 100))
        c1 = (t.totPercent>95) & (t.totPercent<105)
        slic = t[c1&cond][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's4'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_4: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (density out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_5(self):
        """ This is just like set one, except that no carrier purpose is present:
            - looking only at records with valid CAS numbers
            - CASNumber is water
            - 50% < %HFJob < 100% (single 100% records not ok) 
        """
        
        t = self.df[self.df.is_valid_CAS].copy()
        t['has_purp'] = t.Purpose.str.strip().str.lower().isin(self.wlst)
        gbp = t.groupby('UploadKey',as_index=False)['has_purp'].sum()
        t = t.drop('has_purp',axis=1)
        t = pd.merge(t,gbp,on='UploadKey',how='left')
        
    
        c1 = t.has_purp==0  # no records with Purpose in wlst
        c2 = t.bgCAS == '7732-18-5'  # must be water
        c3 = (t.PercentHFJob >= 50)&(t.PercentHFJob < 100)  # should be at least this amount
        #c4 =  t.Purpose.str.strip().str.lower().isin(self.wlst)
        slic = t[c1&c2&c3][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's5'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_5: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_6(self):
        """ Similar to set 1;
            - bgCAS is ambiguousID
            - single record with a carrier purpose
            - IngredientName is either in 'wst' list or has "water" in it
            - 50% < %HFJob < 100% (single 100% records not ok) 
        """
        
        t = self.df.copy()
        t['has_purp'] = t.Purpose.str.strip().str.lower().isin(self.wlst)
        gbp = t.groupby('UploadKey',as_index=False)['has_purp'].sum()
        t = t.drop('has_purp',axis=1)
        t = pd.merge(t,gbp,on='UploadKey',how='left')
        t.TradeName = t.TradeName.str.lower()
        t.TradeName.fillna('empty',inplace=True)
        c1 = t.has_purp==1  # only 1 record with Purpose in wlst
        c2 = t.bgCAS == 'ambiguousID'  # must be water
        c3 = (t.PercentHFJob >= 50)&(t.PercentHFJob < 100)  # should be at least this amount
        c4 =  t.Purpose.str.strip().str.lower().isin(self.wlst)
        c5 = t.IngredientName.isin(self.wlst)|t.IngredientName.str.contains('water')
        c6 = t.TradeName.isin(self.wlst)|t.TradeName.str.contains('water')
        c6 = (~(t.TradeName.str.contains('slick'))) & c6 # prevent 'slickwater' from counting as 'water'
        slic = t[c1&c2&c3&c4&c5&c6][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's6'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_6: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_7(self):
        """ Like set_1, but for salted water:
            - looking only at records with valid CAS numbers
            - single record with a carrier purpose
            - CASNumber is either 7447-40-7 or 7647-14-5
            - 50% < %HFJob < 100% (single 100% records not ok) 
        """
        
        t = self.df[self.df.is_valid_CAS].copy()
        t['has_purp'] = t.Purpose.str.strip().str.lower().isin(self.wlst)
        gbp = t.groupby('UploadKey',as_index=False)['has_purp'].sum()
        t = t.drop('has_purp',axis=1)
        t = pd.merge(t,gbp,on='UploadKey',how='left')
    
        c1 = t.has_purp==1  # only 1 record with Purpose in wlst
        c2 = t.bgCAS.isin(['7447-40-7','7647-14-5'])  # kcl or nacl
        c3 = (t.PercentHFJob >= 50)&(t.PercentHFJob < 100)  # should be at least this amount
        c4 =  t.Purpose.str.strip().str.lower().isin(self.wlst)
        slic = t[c1&c2&c3&c4][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's7'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_7: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_8(self):
        """ Many skytruth carriers have this profile;
            - bgCAS is ambiguousID or 7732-18-5
            - IngredientName is MISSING
            - Purpose is "unrecorded purpose"
            - TradeName has either "water" or "brine"
            - can be one or two records in each disclosure
            - 50% < sum of %HFJob < 100%  
        """
        
        t = self.df.copy()
        #gbp = t.groupby('UploadKey',as_index=False)['has_unrec_purp'].sum()
        #t = t.drop('num_unrec_purp',axis=1)
        #t = pd.merge(t,gbp,on='UploadKey',how='left')
        t.TradeName = t.TradeName.str.lower()
        t.TradeName.fillna('empty',inplace=True)
        c1 = t.Purpose == 'unrecorded purpose'
        c2 = t.bgCAS.isin(['ambiguousID','7732-18-5'])         
        c3 = t.IngredientName=='MISSING'
        c4 = t.TradeName.str.contains('water')
        c4 = (~(t.TradeName.str.contains('slick'))) & c4 # prevent 'slickwater' from counting as 'water'
        tt = t[c1&c2&c3&c4].copy()
        gb = tt.groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gb.columns = ['UploadKey','unrec_percent']
        tt = pd.merge(tt,gb,on='UploadKey',how='left')
        c5 = (tt.unrec_percent >= 50)&(tt.unrec_percent< 100)  # should be at least this amount
        slic = tt[c5][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's8'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_8: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def auto_set_9(self):
        """ Many skytruth carriers have this profile;
            - bgCAS is ambiguousID or 7732-18-5
            - IngredientName is MISSING
            - Purpose is one of the standard carrier words or phrases
            - TradeName has either "water" or "brine"
            - can be one or two records in each disclosure
            - 50% < sum of %HFJob < 100%  
        """
        
        t = self.df.copy()
        t.TradeName = t.TradeName.str.lower()
        t.TradeName.fillna('empty',inplace=True)
        c1 = t.Purpose.str.strip().str.lower().isin(self.wlst) 
        c2 = t.bgCAS.isin(['ambiguousID','7732-18-5'])         
        c3 = t.IngredientName=='MISSING'
        c4 = t.TradeName.str.contains('water')
        c4 = (~(t.TradeName.str.contains('slick'))) & c4 # prevent 'slickwater' from counting as 'water'
        tt = t[c1&c2&c3&c4].copy()
        gb = tt.groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        gb.columns = ['UploadKey','unrec_percent']
        tt = pd.merge(tt,gb,on='UploadKey',how='left')
        c5 = (tt.unrec_percent >= 50)&(tt.unrec_percent< 100)  # should be at least this amount
        slic = tt[c5][['IngredientKey','UploadKey','CASNumber',
                            'IngredientName','Purpose','TradeName',
                             'PercentHFJob','bgCAS','maybe_water_by_MI','dens_test',
                           'MassIngredient','TotalBaseWaterVolume']].copy()
        slic['auto_carrier_type'] = 's9'
        slic['is_new'] = True
        #print(f'Disclosure is in set: {len(slic[slic.UploadKey=="f961a561-edd3-4c9e-8b38-3ba58d2b73c9"])}')
        print(f"Auto_set_9: new {len(slic)}, maybe_water_by_MI? {len(slic[slic.maybe_water_by_MI=='yes'])}, not kept (MIdensity out of range): {len(slic[slic.maybe_water_by_MI=='no'])}")
        slic = slic[~(slic.maybe_water_by_MI=='no')] # don't keep those flagged disclosures
        
        return slic

    def check_for_auto_disc(self):
        results = []
        res = self.auto_set_1()
        self.remove_disclosures(res)
        results.append(res)
                
        res = self.auto_set_2()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_3()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_4()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_5()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_6()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_7()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_8()
        self.remove_disclosures(res)
        results.append(res)

        res = self.auto_set_9()
        self.remove_disclosures(res)
        results.append(res)

        results.append(self.autodf)
        self.autodf = pd.concat(results,sort=True)
        self.autodf.to_csv('./tmp/carrier_list_auto_NEW.csv',quotechar='$',
                           encoding = 'utf-8',index=False)
        #print(f'New auto-detected carriers: {len(slic)}')
        #print(f'Remaining disclosures: {len(self.disc)}')

    def check_auto_against_list(self):
        """ Used to compare what has been identified by auto with another list
        (such as a list from a previous version). Any non-match is found in df
        and saved in a curation-like file..."""
        prev = pd.read_csv('./tmp/mass_keys_v9.csv')
        curr = pd.DataFrame({'UploadKey':self.autodf.UploadKey.unique().tolist()})
        mg = pd.merge(prev,curr,on='UploadKey',how='outer',indicator=True)
        just_prev = mg[mg['_merge']=='left_only'][['UploadKey']] 
        print(f'Number from old (v9): {len(prev)}, current: {len(curr)}, not in new {len(just_prev)}')

        # make out df for review
        # get %HFsums into the list
        self.df = pd.merge(self.df,self.disc[['UploadKey','percSumValid','percSumAll']],
                           on='UploadKey',how='left')

        out = pd.merge(just_prev,self.df[self.df.PercentHFJob>5][['UploadKey','dens_test','maybe_water_by_MI',
                                                                  'TotalBaseWaterVolume','MIok',
                                                                  'CASNumber','bgCAS','IngredientName',
                                                                  'Purpose','PercentHFJob','TradeName',
                                                                  'percSumValid','percSumAll']],
                       on='UploadKey',how='left')
        out = pd.concat([out,out[['UploadKey']]],sort=True)\
            .sort_values(['UploadKey','PercentHFJob'],ascending=False)
        out.to_csv('./tmp/massDisclosures_not_yet_caught.csv')
        
        
    def save_curation_candidates(self):
        c1 = self.df.Purpose.str.strip().str.lower().isin(self.wlst)
        c2 = self.df.PercentHFJob>=5
        t = self.df[c1|c2].copy()
        #t['multi_rec'] = t.UploadKey.duplicated(keep=False)
        t['is_new'] = True
        t['cur_carrier_status'] = ''
        t['is_water_carrier'] = ''
        ukt = t.UploadKey.unique().tolist()
        
        print(f'Still to be curated (all data sets): {len(t.UploadKey.unique())} ')
        self.curdf.is_new = False
        self.curdf = pd.concat([self.curdf,t],sort=True)
        # add blank line to make excel curation easier
        self.curdf = pd.concat([self.curdf,pd.DataFrame({'UploadKey':ukt})],sort=True)
        self.curdf = self.curdf.sort_values(['UploadKey','PercentHFJob'],ascending=False)
        self.curdf[['UploadKey','IngredientKey','is_new','is_valid_CAS',
                    'TotalBaseWaterVolume','CASNumber','bgCAS','IngredientName',
                    'Purpose','TradeName','PercentHFJob','percSumValid','percSumAll',
                    'maybe_water_by_MI','is_water_carrier',
                    'cur_carrier_status','first_date',
                    'change_date','change_comment']].to_csv('./tmp/carrier_list_curated_NEW.csv',quotechar='$',
                          encoding='utf-8',index=False)
        
    def remove_disclosures(self,sourcedf):
        if self.remove_dropped_keys:
            upk = sourcedf.UploadKey.unique().tolist()
            self.df = self.df[~(self.df.UploadKey.isin(upk))]
            self.disc = self.disc[~(self.disc.UploadKey.isin(upk))]
            #print(f'In length: {len(upk)}; After removing: df: {len(self.df)}; disc: {len(self.disc)}')
        
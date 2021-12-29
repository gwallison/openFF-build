# README for open-FF repository and project

This CodeOcean capsule is a system of code to transform data from 
the online chemical disclosure site 
for hydraulic fracturing, FracFocus.org, into a usable database.  Currently,
these data include over 6,000,000 chemical records in over 175,000 fracking events.

The code performs cleaning, flagging, and 
curating techniques to yield organized data sets and sample analyses 
from a difficult collection of chemical records.   
For a majority of these records, the **mass** of the chemicals used 
in fracking operations is calculated. 

The output of this project includes full data sets and filtered data sets. All 
sets include many of the original raw FracFocus fields and many generated
fields that correct and add context to the raw data.  Filtered data sets remove
the FracFocus records that have significant problems, which gives the user a 
product that is usable without much work.  On the other hand, full sets do not 
filter out any of the original raw FracFocus records allowing 
the user to construct their own appropriate set (by using the flags, etc.).  

Portions of the raw bulk data that are filtered out include: 
- fracking events with no chemical records (mostly 2011-May 2013; but are 
    replaced with the SkyTruth archive).
- fracking events with multiple entries (and no indication which entries 
    are correct).
- chemical records that are identified as redundant within the event.

Additionally, the mass for reported chemicals is calculated **only** for those disclosures
that meet more stringent criteria, such as consistent percentages, base water volume,
and a match with MassIngredient field (when available).

Finally,  we clean up some of the labeling fields by consolidating multiple 
versions of a single category into an easily searchable name. For instance, 
we collapse the 80+ versions of the supplier name 'Halliburton' to a single
value 'halliburton'.

This code is designed to facilitate adding new disclosures to data sets 
periodically (after curation) to keep the output data sets relatively 
up to date. 

By removing or cleaning the difficult data from this unique data source, 
we have produced a data set that can facilitate more in-depth 
analyses of chemical use in the fracking industry.

Find the github repositories of code at:
- [openFF-build](https://github.com/gwallison/openFF-build)
- [openFF-curation](https://github.com/gwallison/openFF-curation)


## CodeOcean Versions of Open-FF 

** after v 11 BETA **:

- Added testing module to confirm consistency of final data sets.

- Changed calcMass values of zero to NaN to indicate they are a non-disclosed 
    quantity (Dec. 20,2021)
    
- in DataDictionary, fixed massCompFlag description to reflect that it is **True** when
    massComp is **out of** tolerance.
    
- mark disclosures without chemical records as out-of-tolerance for to percent.

- add hash checks on repository files (Dec 29, 2021)

**Version 11 **:

- Data downloaded from Dec. 4, 2021. 

- cleanMI field added that is MassIngredient with values that are inconsistent at
    the disclosure level removed.

- Added carrier detection sets: set2, set3, set4 and set5. This adds about 25,000
    disclosures that are eligible for mass calculations. 
    
- Flagged SkyTruth-based disclosures that have been deleted from FracFocus's pdf
    database (n=140).  They are no longer included in the standard filtered data set,
    but can still be accessed in the Full data set.  See the list at /data/ST_api_without_pdf.csv

- Changed the file structure of the project to be more in line with python
    package structure. Started new github repository for this branch (openFF-build).
    
- Removed clusterID and other fields that were experimental or not used.

    
**Version 10 - MAJOR REVISION**:

- Data downloaded from Oct 10, 2021. Updated versions of the data may be periodically 
    available without code changes.  See the [Open-FF blog](https://frackingchemicaldisclosure.wordpress.com/).

- Chemical identification is now curated using both CASNumber and IngredientName instead of by 
    automated analysis of CASNumber only. This includes separate evaluation of the
    two fields using comparisons to authoratative references (Chemical Abstracts's "SciFinder" and 
    the EPA's "CompTox". This
    change allows for many simple typos to be corrected and for many obviously
    wrong CASNumbers to be changed or flagged as 'ambiguous'.  Further, this curation
    produces a more thorough characterization of proprietary claims and the
    'category' of the change is available to the end user for further analysis.
    The translation file ("casing_curated.csv") between original CASNumber/IngredientName pairs and
    resulting bgCAS is in the /sources directory ("/data" folder for CodeOcean).
    
- The carrier records of a disclosure are now determined by combination of automation
    and manual curation (instead of just automation as in previous versions). The
    code introduced here allows many more disclosures to be included in mass calculations.
    This version does not yet apply the manual curation, but the version adds the
    functionality.
    
- The code and reference files created to translate SciFinder CAS naming is
    now included in the core file "process_CAS_ref_files.py."
    
- The code used to do pre-processing can be found in the /builder_tasks folder, 
    though it is not performed in CodeOcean. It is included for completeness
    and transparency.  The output of these scripts are mostly held in the 
    /data (/sources) folder.
    
- The calculation of mass for every chemical now incorporates the density of the
    carrier fluid indicated in the IngredientComments field, when available. This is
    currently available for about 24,000 disclosures.
    
- The calculation of mass now is checked against the undocumented but informative
    FracFocus field, MassIngredient.  While this field is only available for a subset of disclosures and
    can be internally inconsistent, when checked against calcMass, we can find (and filter)
    records or whole disclosures that may be more error prone.
    
- ClusterID changed to fixed length string for more consistent searches.

- Individual boolean flags are available for fine-grained filtering in the full data set.

- Classes of code to create data sets in a consistent manner across different
    bulk data inputs are available in the Analysis_set class and subclasses.  The
    output from the CodeOcean run includes two zipped data set from Analysis_set.
    
- Text indications of missing values in CASNumber, IngredientName, and Supplier
    have been consolidated into the single token: "MISSING." See the tranlation table,
    \data\missing_values.csv.
    
- We include a more comprehensive and searchable Data Dictionary with tables
    showing components of canned data sets.

- New external reference data sets have been added: The Clean Water Act list as 
    curated by the EPA, the EPA's "Drinking Water Standard and Health Advisories Table",
    EPA's master PFAS list and 
    EPA's list of volatile chemicals. These fields, 'is_on_CWA', 'is_on_DWSHA',
    'is_on_PFAS_list' and 'is_on_volatile_list',
    can be used to quickly identify those groups of chemicals.

- Large-scale refactoring of code has removed unused or overly complicated 
    sections.

**Version 9**: 
- Data download from FracFocus on March 5, 2021.
- Correct FF_stats.py calculation for percent non-zero in the integer
    and float section. 
- Generate geographic clusters as proxy of wellpad identity;
    clusters are found in the string field "clusterID". (Note that a specific clusterID will NOT be
    consistent across data set versions in the way that UploadKey is; don't depend
    on it!).  
- The fields FederalWell and IndianWell have been changed to string type -
    previously, they were boolean (T/F) but that type does not allow for empty
    cells which occurs in the SkyTruth data, leading to misinformation. 
- Added PercentHighAdditive to full data output to allow for better investigations
    of TradeName usage. 
- Rename the old field 'infServiceCo' to 'primarySupplier' to
    better reflect its generation.
- Added chemical lists of the Clean Water Act, the Safe Drinking Water Act,
    and from California's Proposition 65 lists to help identify chemicals of
    concern.

**Version 8.1**: Correct slight documentation omission.

**Version 8**: Added WellName field to filtered data output.  Added chemical ingredient
   codes from the WellExplorer project (www.WellExplorer.org) -- fields with
   the prefix 'we_' are from that project. See https://doi.org/10.1093/database/baaa053
   Data download from FracFocus on October 23, 2020.

**Version 7**: Data downloaded from FracFocus on July 31, 2020.  TradeName added
   to exported data.

**Version 6**: Save data tables in pickled form into results section so that it may be
   exported to other projects.

**Version 5**: Data downloaded from FracFocus on May 14, 2020.  No other changes.

**Version 4**: Data downloaded from FracFocus on March 20, 2020.  Added the generated
   field, infServiceCo. This field is an attempt to identify the primary
   service company of a fracking event.  Including in the output files the 
   raw field 'Projection' which is needed to accurately map using lat/lon
   data.

**Version 3**: Data downloaded from FracFocus on Jan. 22, 2020. Modified the 
   FF_stats module to generate separate reports for the "bulk download" and
   the "SkyTruth" data sources.  Both are reported in "ff_raw_stats.txt" in the
   results section.)

**Version 2**: Data downloaded from FracFocus on Jan. 22, 2020. Incorporated 
   basic statistics on the raw FracFocus data (see "ff_raw_stats.txt" in the
   results section.)

**Version 1**: Data downloaded from FracFocus on Jan. 22, 2020. Similar 
   to the Proof-of-Concept version with the following new features:
   SkyTruth archive
   has been incorporated.  Links to references include: Elsner & Hoelzer 2016, 
   TEDX chemical list and TSCA list.



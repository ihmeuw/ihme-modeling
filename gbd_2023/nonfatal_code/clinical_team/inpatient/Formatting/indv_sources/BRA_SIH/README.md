# BRA_SIH formatting

In November 2019 the formatting for the Clinical Informatics inpatient source BRA_SIH was redone.  This describes the changes to formatting and other information about this source.

See [here](ADDRESS) for the GHDx entries on this source.

## Code structure

As part of the November 2019 reformat, code was moved to a new location. Before, the many scripts were scattered throughout the Formatting directory. Now they are collected within one directory for the source BRA_SIH, with a subdirectory for old Stata scripts.

### File and directory structure
```
FILEPATH
├── 001_pre_format_BRA_SIH_15_16.py
├── 01_format_BRA_SIH_15_16.py
├── create_files_list.py
├── format_BRA_SIH_master.py
├── format_BRA_SIH.py
├── old_BRA_SIH
│   ├── 01_format_BRA_SIH.do
│   └── prep_BRA_SIH.do
└── README.md
```

five files are currently used: `001_pre_format_BRA_SIH_15_16.py`, `01_format_BRA_SIH_15_16.py`, `format_BRA_SIH_master.py`, `create_files_list.py`, and `format_BRA_SIH.py`. `001_pre_format_BRA_SIH_15_16.py` and `create_files_list.py` is not normally ran.

The most important script is `format_BRA_SIH_master.py`. It qsubs two worker scripts that process different years of data. In the end it compiles and processes all years of data.

### Brief description of script relationships

- `001_pre_format_BRA_SIH_15_16.py`
  - Unchanged in November 2019 reformat
  - Only runs on 2015 and 2016
  - Reads raw data files and does light formatting
- `01_format_BRA_SIH_15_16.py`
  - Unchanged in November 2019 reformat (except to rename output file)
  - Uses the outputs of `001_pre_format_BRA_SIH_15_16.py`
  - Only runs on 2015 and 2016
  - Finishes formatting for 2015 and 2016
- `format_BRA_SIH_master.py`
  - This is the main script
  - New as of the November 2019 reformat
  - This script launches `format_BRA_SIH.py` and `01_format_BRA_SIH_15_16.py`, and picks up their outputs.
  - Finishes formatting for all years of data 1997 - 2016
- `format_BRA_SIH.py`
  - New as of the November 2019 reformat
  - Runs years of data 1997 - 2014.
  - Reads raw data files and does light formatting
- `create_files_list.py`
  - Generates a list of data files to process

## Code structure before reformatting

Formatting for BRA_SIH was split into two overall chunks, one for 1997-2014 and another for 2015-2016. The earlier chunk was written in Stata around 2015, and the second one was written in Python in 2017.

In brief, **the reformat replaced the Stata scripts.**

- 1997 - 2014
  - `prep_BRA_SIH.do`
    - ran first and pre-processed the raw data files
  - `01_format_BRA_SIH.do`
    - ran second using outputs of `prep_BRA_SIH.do`
- 2015 - 2016
  - `001_pre_format_BRA_SIH_15_16.py`
    - ran first and pre-processed the raw data files
  - `01_format_BRA_SIH_15_16.py`
    - ran second using outputs of `001_pre_format_BRA_SIH_15_16.py`


## Data locations

### 1997 - 2014
The data for the years 1997 - 2014 is stored in `FILEPATH`. Inside this directory there are four directories that each cover a range of years. Inside those, there is a subdirectory for each State in Brazil. Inside those, there are subdirectories for each year of data. Inside those there are several data files with the extension `.DBF`.

For more info on DBF files, see [here.](https://en.wikipedia.org/wiki/.dbf)

Tip: This DBF files are not shapefiles. Also, you can open them with Microsoft Excel!

#### Folder structure for raw data
```
FILEPATH
├── RD_1992_1999
├── RD_2000_2007
├── RD_2004_2013
└── RD_2008_2014
    ├── ACRE
    ├── ALAGOAS
    ├── AMAPA
    ├── AMAZONAS
    ├── BAHIA
    ├── CEARA
    ├── ESPIRITO_SANTO
    ├── FEDERAL_DISTRICT
    ├── GOIAS
    ├── MARANHAO
    ├── MATO_GROSSO
    ├── MATO_GROSSO_DO_SUL
    ├── MINAS_GERAIS
    ├── PARA
    ├── PARAIBA
    ├── PARANA
    ├── PERNAMBUCO
    ├── PIAUI
    ├── RIO_DE_JANEIRO
    ├── RIO_GRANDE_DO_NORTE
    ├── RIO_GRANDE_DO_SUL
    ├── RONDONIA
    ├── RORAIMA
    ├── SANTA_CATARINA
    ├── SAO_PAULO
    ├── SERGIPE
    └── TOCANTINS
        ├── 2008
        ├── 2009
        ├── 2010
        ├── 2011
        ├── 2012
        ├── 2013
        └── 2014
            ├── BRA_SIH_2014_RDTO1401_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1402_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1403_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1404_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1405_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1406_Y2014M11D04.DBF
            ├── BRA_SIH_2014_RDTO1407_Y2014M11D04.DBF
            └── BRA_SIH_2014_RDTO1408_Y2014M11D04.DBF
```

### 2015 - 2016
raw data for 2015 is stored at `FILEPATH`, and 2016 is at `FILEPATH`. The directory structure for these years is simpler. There are no subdirectories.  They are also `.DBF` files.

## Reading in data / handling duplicated files

**Not all data in these directories are read in**. Notice that there is an overlap in the years covered by these directories:
```
FILEPATH
├── RD_1992_1999
├── RD_2000_2007
├── RD_2004_2013
└── RD_2008_2014
```

The year range 2004-2013 overlaps. The year range in `RD_2004_2013` is already covered by other directories. There are duplicated files.  That is, the same exact file, with the same exact data, sometimes appears in more than one location in the directories for this data source.  Reading in every file in the directories for this source would lead to over-counting.  Over-counting would be a big problem for our estimation process, and so would undercounting, so it's very important to use exactly the right data.

**All the files in `RD_2004_2013` are excluded.**

There are duplicated files between `RD_1992_1999` and `RD_2000_2007`, and between `RD_2004_2013` and `RD_2008_2014`. We cannot simply exclude the files under `RD_2004_2013`.  We also have to check for duplicates between the other directories.  Sometimes there are year directories inside and RD directory that don't match the year range in the RD directory name. For example, 2002 inside `RD_1992_1999`.

This was determined by testing all the files in the directories for this source.

The script `create_files_list.py` identifies which files are duplicates and creates list of which files should be processed and saves them by year in a location where `format_BRA_SIH.py` can find them.

Here's how it works:
The structure of the data storage directory on `/limited_use/` is based on a year range, then state, then a specific year. The scripts uses that structure.

It gets a unique list of file names in **one** given RD directory (i.e, RD_1992_1999 or RD_2004_2013). Once it has that list, it looks for a file of the same file name, with the same state and year bath, in a _different_ RD directory. For example, these two files have the same file name, state, and year, but a different RD directory:

1. FILEPATH
2. FILEPATH

The only difference in between the two file paths are the different RD directories, RD_2000_2007 and RD_2004_2013.

These are treated as potential duplicates. Potential duplicates come in pairs. But, not all files have a potential duplicate.

Then, within each potential duplicate the two files are compared to each other. Some of the potential duplicated pairs are actually not duplicates, and contain different data.

By looking through each state, year, and across RD files, a list of lists of all the files in the source data storage directory is made. This is called `master_list` in the code. Inside master list, are lists of length one, or of length two. The entities in these sub-lists are full filepaths. The length of a sublist is one when there is not a potential duplicate and two when there is a potential duplicate.

Next, among the true duplicates, one of each pair is arbitrarily chosen to be included, and the other to be excluded.

Next, one of each pare of true duplicates is dropped from the master list. This gives the file list of files to include in formatting. The files are then split by year and saved.

The test for duplicates has two steps:
1. Test the two potential duplicates with `filecmp.cmp()`. This function uses the linux command `stat` under the hood to see if the `stat` values are identical for the two files. If `stat` is different, then it compares the data by reading the bytes of the files. See the man page for `stat`, the docstring for `filecmp.cmp()`, or the comments in our code for details.
2. If in step 1. the test said that the files were not identical, a further test is performed. The data is read in with Pandas and only the columns that we would actually use are kept. That is about 10 columns out of about 40. Then the two dataframes are compared with `pandas.equals()`. In many cases, the files that were not identical in step 1. are actually identical in the columns that we actually use.

**Note**: in this algorithm, file paths that look similar, and only differ in the RD directory, as in the example above, _are both included if the data are not identical_.

to be clear, both files
1. FILEPATH
2. FILEPATH

would both be read in and included in formatting if the data within them are not identical.


### Alternative methods of choosing files

Other methods were considered and tested:

1. Just using filecmp.cmp() to compare and find duplicates
2. Using filecmp.cmp() and pd.equals()  on select columns to compare and find duplicates
3. Using filecmp.cmp() and pd.equals()  and excluding the folder RD_2004_2013

We currently use method 3. The differences between the three were minimal, but method three made the most sense, so it was chosen over the others.

## Data locations before reformatting

In the past, the limited use directory was inside of `J:/DATA`. It got moved to `/ihme/limited_use`. The older Stata codes read from the old location on `J:/DATA/limited_use`.  Therefore, the original location of data that the Stata scripts read from no longer exists.

### Data file types before reformatting.

One of the biggest differences between pre-reformat and post-reformat is the file types of the raw data. From looking at the old Stata scripts, it's clear that they read in data with the file extension `.DTA`. But in the current location of the raw data on `/ihme/limited_use`, all the files have the extension `.DBF`.

Presumably, the DTA files where conversion of the DBF files. We don't know what happend to the DTA files.

Bottom line, we're no longer reading in the same files that were used before. Probably, the data within the files are the same, but there's no way to know for sure.

## Changes after reformatting

One of the goals of the reformat was to add new age groups in ages under 5 years old. Those changes were accounted for when comparing different versions, and are excluded from discussion here.

One of the most major changes was in the year 1997.  That data is currently identified as ICD9. Before it was treated as ICD 10. Before reformatting 1997 had about 200,000 cases. Now it has about 11 million, which is in line with the number of cases in the rest of the years. Clearly, 1997 used to have far too few cases.

When the old version of 1997 and new version of 1997 are compared, it's clear that the old version has ICD10 codes and the new version has ICD 9 codes.  There's no way of knowing for sure why that is the case since the file locations of the original data moved (e.g., DBF vs DTA files, etc.). But, because of this 1997 looks completely different due to the reformat. And, since the total number of cases for the year looks better, the changes seem to be better overall.

In the rest of the years, there are also significant changes, though not as large as in 1997. The overall case counts across all years besides 1997 are similar. The distribution of cases by year is similar.

Beyond that, there are many new changes. When comparing age, sex, year, location, diagnosis number, outcome, and ICD code there are many new combinations of these. Some of these combinations are missing and new ones were added.  This could be attributed to the different set of files being read in.

2015 and 2016 remain identical before and after the reformat since nothing was done to them besides add new age groups.

Overall, despite the differences, the reformatted data is better since it no longer reads in duplicated files, and 1997 has improved.


## Other Changes
- The old data for 1997-2014 did not distinguish between deaths and a discharge. Everything was labeled as a "case". The reformat added deaths/discharges as outcomes.
- The location mapping used for the years 1997-2014 was wrong. It had been that way since probably GBD 2015.
- 2015 and 2016 had not implemented the procedure that swaps Injuries / "E-Codes" from secondary diagnosis positions into the primary diagnosis posistion. So until the reformat those years were missing a lot of injuries data.

## Choosing Location for `location_id`
Some years of this source have more than one location recorded. There's the municipality of residence of the patient, and the municipality where the hospital is located. Municipality is more geographically detailed than State. The first two digits of the municipality coded value encode the State.  In the data, municipality of residence of the patient is called `MUNIC_RES` and municipality of the hospital is `MUNIC_MOV`.

Presumably most of the data sources we use only provide location of the hospital. However, we would prefer to use location of the patient wherever possible. For example, if someone in Vancouver WA gets sick, they’ll probably go to a hospital in Portland instead of Seattle since it's much closer, even though we would want to consider them in the WA population. So, in most of the years where both are available, the code selects the location of the patient.

There is one exception. The year 1994 has both kinds of locations available. The generally preferred location, residence, is null 91% of the time. In this case location of hospital was used.

## misc
- 2014 has a significantly lower level of admissions. This is due to the fact that there are fewer raw data files for this year.
- Documentation on the raw data can be found in these locations:
FILEPATH

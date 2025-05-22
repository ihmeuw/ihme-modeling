## Age-sex data README

**Major output**: input_data.dta (

## Order of scripts:
**00_cbh.R**
  - Compiles old and new CBH
- Drops missing 5q0
- Calculates p's and q's
- Fills missing values by aggregating smaller age groups
- Removes duplicates
- Outputs a list of missing cbh .dta files
- **Intermediate output**: compiled_cbh.csv

**00_noncod_vr.R**
  - Run in parallel with 00_cbh.R
- Noncod VR

- Subsets to under 5 age groups
- Creates implied aggregate age group deaths from granular age groups for VR data
- Checks to ensure we have one source for a location/year/sex/age
- Splits-off nonVR from noncod VR file

- Cod VR

- Subsets to under 5 age groups
- Creates implied aggregate age group deaths from granular age groups
- Removes duplicates		
- Aggregates subnational deaths to national locations (if missing)

- Combines noncod and cod

- Merge cod and noncod
- Prioritizes cod over noncod

- Combines vr, srs, and dsp	

- Append noncod nonvr	

- Removes unknown sex and existing both sexes
- Generates both sexes from male and female data
- Generate implied u5 deaths for all sources
- **Intermediate output**: cod_noncod_vr.csv

**01_compile_agesex_data.R**
  - Compiles population data (with exceptions for IND SRS and CHN WHO data)
- Calculates risks for VR (i.e. qx and conditional probabilities)
- Scale VR pna/pnb and cha/chb to respective aggregate age groups
- Compiles VR, CBH, and SRS (i.e. appends all data)
- Marks data types (i.e. age format)
- Makes transformations (i.e. conditional probabilities)
- **Intermediate output**: compiled_data.csv
	
**02_outliering.R**
- Mark outliers and exlcusions based on 5q0 completeness, 45q15, survey bias, etc.
- Carries over some data drops from old code
- Output final .dta file for age-sex model (age-sex model expects a Stata file)

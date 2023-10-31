# 5q0 data prep steps

### 01: combine_all_child_sources.R

- Read in all sources of 5q0 data (CBH, SBH, pre-kids DDM)
- Calculate 5q0 from VR
	- Pull deaths data from d00_compile_empirical_deaths.dta, produced by DDM
	- Pull population from d09_denominators.dta, in DDM
		- Population = empirical population data + population estimates, both sex only
	- Pull births from live births process
	- If possible, find 5m0 using deaths/population, then convert to 5q0
	- Else, find 5q0 using deaths/ births
- Deduplication
- Manual scrubbing
- Identify indirect calculations based on 15-19 and 20-24 year old women, drop 2 most recent
- Only keep standard locations

### 02: add_nids.R

- Attach NIDs, from "FILEPATH" and `FILEPATH","FILEPATH"
- Bring in country-specific NIDs; US, GBR, BRA, and remainder from WHO mortality database

### 03: format_for_upload.R

- Add on source type IDs
- Deduplication
- Identify sources with SBH DHS only, no CBH

### 04: 04_upload_5q0.R

- Upload data
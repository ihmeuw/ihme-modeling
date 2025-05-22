Configuration guide
===================

Age Configuration
-----------------
- **AgeDataConfig** :
    - **index**: a list of columns that makes each row unique (e.g., seq) *and* contains columns like location, year, and sex for later merging.
    - **age_lwr**: lower bound of age for a given row.
    - **age_upr**: upper bound of age for a given row.
    - **val**: the value you want split.
    - **val_sd**: the standard error for the measure you want split.

- **AgePatternConfig** :
    - **by**: a list of columns you want to match the data to (how specific is your pattern in addition to the age_key?).
    - **age_key**: age_group_id (at IHME).
    - **age_lwr**: lower bound of a given age_key (can merge "age_group_years_start/end" from get_age_metadata if needed).
    - **age_upr**: upper bound of a given age_key.
    - **draws**: a list of draw columns (e.g., "draw_1", ... "draw_n").
    - **val** & **val_sd**: alternative to draws if you have a mean_draw and standard error. Otherwise will become the

- **AgePopulationConfig** :
    - **index**: a list of columns that makes each row unique (e.g., age_group_id, location_id, year_id, sex_id).
    - **val**: the population value.

Sex Configuration
-----------------

- **SexDataConfig** :
    - **index**: a list of columns that makes each row unique (e.g., nid, seq) *and* contains columns like location, year, and age for later merging.
    - **val**: the value you want split.
    - **val_sd**: the standard error for the measure you want split.

- **SexPatternConfig** :
    - **by**: a list of columns you want to match the data to (how specific is your pattern?).
    - **val** & **val_sd**: the mean and standard error of the pattern.

- **SexPopulationConfig** :
    - **index**: a list of columns that makes each row unique (e.g., location_id, year_id).
    - **sex**: the column name for sex.
    - **sex_m**: the value representing male.
    - **sex_f**: the value representing female.
    - **val**: the population value.

Categorical Configuration
-------------------------

- **CatDataConfig** :
    - **index**: a list of columns that makes each row unique (e.g., study_id, year_id, location_id).
    - **cat_group**: the categorical group column.
    - **val**: the value you want split.
    - **val_sd**: the standard error for the measure you want split.

- **CatPatternConfig** :
    - **by**: a list of columns you want to match the data to (how specific is your pattern?).
    - **cat**: the categorical column.
    - **draws**: a list of draw columns (e.g., "draw_1", ... "draw_n").
    - **val** & **val_sd**: alternative to draws if you have a mean and standard error.

- **CatPopulationConfig** :
    - **index**: a list of columns that makes each row unique (e.g., year_id, location_id).
    - **val**: the population value.
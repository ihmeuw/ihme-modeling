==========
Quickstart
==========

Below are examples of how to load the package in Python and R. The Python example shows age-splitting, sex-splitting, and categorical splitting, and the R example shows the same.

.. _quickstart-python:

Python
======

Import Example
~~~~~~~~~~~~~~
.. code-block:: python

    from pydisagg.ihme.splitter import (
        AgeDataConfig,
        AgePatternConfig,
        AgePopulationConfig,
        AgeSplitter,
        SexDataConfig,
        SexPatternConfig,
        SexPopulationConfig,
        SexSplitter,
        CatDataConfig,
        CatPatternConfig,
        CatPopulationConfig,
        CatSplitter,
    )

.. _quickstart-age-splitting:

Age Splitting
~~~~~~~~~~~~~
.. code-block:: python

    # Age Splitting Configuration
    fpg_data_con = AgeDataConfig(
        index=["unique_id", "location_id", "year_id", "sex_id"],
        age_lwr="age_start",
        age_upr="age_end",
        val="mean",
        val_sd="SE",
    )

    # draw_cols assumes you use a draw based approach, if you have a point estimate 
    # and standard error those can be passed into the config directly (i.e. "mean_draw")
    
    #draw_cols = patterns_fpg.filter(regex="^draw_").columns.tolist()

    fpg_pattern_con = AgePatternConfig(
        by=["location_id", "year_id", "sex_id"],
        age_key="age_group_id",
        age_lwr="age_group_years_start",
        age_upr="age_group_years_end",
        #draws=draw_cols,
        val="mean_draw",
        val_sd="var_draw",
    )

    fpg_pop_con = AgePopulationConfig(
        index=["age_group_id", "location_id", "year_id", "sex_id"],
        val="population",
    )

    age_splitter = AgeSplitter(
        data=fpg_data_con, pattern=fpg_pattern_con, population=fpg_pop_con
    )

    result_age = age_splitter.split(
        data=fpg_df,
        pattern=patterns_fpg,
        population=pops_df,
        model="logodds",
        output_type="rate",
    )

.. _quickstart-cat-splitting:

Categorical Splitting
~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    # Categorical Splitting Configuration
    cat_data_con = CatDataConfig(
        index=["study_id", "year_id", "location_id"],
        cat_group="location_id",
        val="mean",
        val_sd="std_err",
    )

    cat_pattern_con = CatPatternConfig(
        by=["year_id"],
        cat="location_id",
        val="mean",
        val_sd="std_err",
    )

    cat_pop_con = CatPopulationConfig(
        index=["year_id", "location_id"],
        val="population",
    )

    cat_splitter = CatSplitter(
        data=cat_data_con, pattern=cat_pattern_con, population=cat_pop_con
    )

    result_cat = cat_splitter.split(
        data=pre_split,
        pattern=data_pattern,
        population=data_pop,
        model="rate",
        output_type="rate",
    )

.. _quickstart-sex-splitting:

Sex Splitting
~~~~~~~~~~~~~
.. code-block:: python

    # Sex Splitting Configuration
    sex_data_con = SexDataConfig(
        index=["nid", "seq", "location_id", "year_id", "sex_id", "age_lwr", "age_upr"],
        val="val",
        val_sd="val_sd",
    )

    sex_pattern_con = SexPatternConfig(
        by=["year_id"],
        val="draw_mean",
        val_sd="draw_sd",
    )

    sex_pop_con = SexPopulationConfig(
        index=["location_id", "year_id"],
        sex="sex_id",
        sex_m=1,
        sex_f=2,
        val="population",
    )

    sex_splitter = SexSplitter(
        data=sex_data_con, pattern=sex_pattern_con, population=sex_pop_con
    )

    result_sex = sex_splitter.split(
        data=pre_split,
        pattern=sex_pattern,
        population=sex_pop,
        model="rate",
        output_type="total",
    )

.. _quickstart-r:

R
=

R Import Example
~~~~~~~~~~~~~~~~
.. code-block:: r

    library(reticulate)
    reticulate::use_python("/some/path/to/miniconda3/envs/your-conda-env/bin/python")
    splitter <- import("pydisagg.ihme.splitter")

.. _quickstart-r-age-splitting:

R Age Splitting
~~~~~~~~~~~~~~~
.. code-block:: r

    # Age Splitting Configuration
    age_splitter <- splitter$AgeSplitter(
        data=splitter$AgeDataConfig(
            index=c("unique_id", "location_id", "year_id", "sex_id"),
            age_lwr="age_start",
            age_upr="age_end",
            val="mean",
            val_sd="SE"
        ),
        pattern=splitter$AgePatternConfig(
            by=c("location_id", "year_id", "sex_id"),
            age_key="age_group_id",
            age_lwr="age_group_years_start",
            age_upr="age_group_years_end",
            draws=draw_cols,
            val="mean_draw",
            val_sd="var_draw"
        ),
        population=splitter$AgePopulationConfig(
            index=c("age_group_id", "location_id", "year_id", "sex_id"),
            val="population"
        )
    )

    result_age_df <- age_splitter$split(
        data=fpg_df,
        pattern=patterns_fpg,
        population=pops_df,
        model="logodds",
        output_type="rate"
    )

Basic split_epi_model usage
================================

This page gives an overview of how to use split_epi_model to split an epi model.


split_epi_model
-------------------

Splits modelable entity draws based on proportion models. Outputs are new draw files for each proportion model, tagged with the target_meid. Function directly outputs a dataframe containing the output directory where either the draws or an errors logfile can be found.

Arguments:

    - source_meid(int): The modelable entity id of the me to be split.
    - target_meids(List[int]): A list of modelable entity ids that will be used to tag the new draws.  Essentially, these are the me_ids for the newly created models.
    - prop_meids(List[int]): A list of modelable entity ids corresponding to the proportion models used to do the split.
    .. note::
        target_meids and prop_meids need to have 1-1 parity with each other and are order dependent. Example: target_meids = [2891, 2892, 2893, 2894] prop_meids = [1922, 1920, 1921, 1923] Proportion model 1922 will be used to create model 2891, proportion model 1920 will be used to create model 2892 and so on.
    - split_measure_ids(int or List[int], optional): A list of measure_ids from the source model, to be split according to the proportion models.  Default is [5, 6], prevalence and incidence.
    - prop_meas_id(int, optional): The measure id used in the proportion models. Default is 18, proportion.
    - output_dir(str, optional): The directory where the draw files are created. Subdirectories are created for each target modelable entity id, and files are tagged by location id. Example: output_dir/2891/35.h5 is a draw file for newly created model 2891, location_id 35. Default is "FILEPATH/{given gbd_round}".
    - decomp_step (str, optional): The decomposition step used to query the appropriate source model versions and proportion model versions for splitting. Defaults to None. Will raise an error if not a valid decomp_step argument for the given GBD round. Possible values are 'iterative', 'step1', 'step2', 'step3', 'step4', or 'step5'.
    - gbd_round_id(int, optional): The gbd_round_id for the proportion models being used. This argument is used to retrieve the best model for a given round. Default is the current GBD round.
    - proportion_meid_decomp_steps (List[str] or str, optional): Users may want input proportion me_ids from a decomp_step different from their source epi model's. proportion_meid_decomp_steps tells split_epi_model where its target proportion me_ids are saved in decomp. Defaults to None, where split_epi_model uses the plain decomp_step argument to locate both proportion model and the source model estimates. Can be a list of values that positionally map to the list of proportion_meids, as seen in the python example.

Returns:
    Dataframe containing the output directory where either the draws or an errors logfile can be found.


Resource Requirements
*****************

Split model requires quite a bit of cluster resources to run. To prevent failure due to not requesting the correct number of cores or memory, we strongly recommend requesting at least **30 threads and 60 GB of memory**.


Example usage
*****************


Here's three examples of calling split_epi_model.

Python, positional mapped proportion_meid_decomp_steps:

.. code::

    In [1]: from split_models.split_epi import split_epi_model
    In [2]: source_meid = 1919
    In [3]: target_meids = [99991, 99992, 99993, 99994]
    In [4]: prop_meids = [1922, 1920, 1921, 1923]
    In [5]: prop_steps = ['iterative', 'step2', 'iterative', 'iterative']
    In [6]: df = split_epi_model(source_meid=source_meid,
                                 target_meids=target_meids,
                                 prop_meids=prop_meids,
                                 gbd_round_id=7,
                                 decomp_step='step1',
                                 proportion_meid_decomp_steps=prop_steps)


R, use one decomp_step for all proportion models:

.. code::

    > source("FILEPATH/split_epi_model.R")

    > source_meid <- 1919

    > target_meids <- c(99991, 99992, 99993, 99994)

    > prop_meids <- c(1922, 1920, 1921, 1923)

    > data <- split_epi_model(source_meid=source_meid,
                              target_meids=target_meids,
                              prop_meids=prop_meids,
                              gbd_round_id=6,
                              decomp_step='step1',
                              proportion_meid_decomp_steps='iterative')


Stata, use same decomp_step for source and proportions:

.. code::

    . run "FILEPATH/split_epi_model.ado"

    . local source_meid 1919

    . local target_medis 99991 99992 99993 99994

    . local prop_meids 1922 1920 1921 1923

    . split_epi_model, source_meid(`source_meid') target_meids(`target_meids') prop_meids(`prop_meids') gbd_round_id(6) decomp_step(step1) clear

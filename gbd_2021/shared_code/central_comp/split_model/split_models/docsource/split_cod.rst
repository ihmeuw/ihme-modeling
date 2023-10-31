Basic split_cod_model usage
================================

This page gives an overview of how to use split_cod_model to split a cod model.


.. note::

    Split_cod_model can only be run on the cluster!


split_cod_model
-------------------

Splits cause draws. Outputs are new draw files for each target cause. Function directly outputs a dataframe containing the output directory where either the draws or an errors logfile can be found.

.. note::

    GBD 2020 STGPR and DisMod proportion models will always be sourced from decomp_step 'iterative'. Please see the GBD2020 decomposition calendar for more background: URL.

Arguments:

    - source_cause_id (int): The cause_id to be split into target_cause_ids.
    - target_cause_ids (List[int]): The cause_ids that should be produced as a result of the split. These should be provided in the same order as the target_meids which define the split proportions.
    - target_meids (List[int]): The modelable_entity_ids containing the proportion models by which to split the source_cause_id. These should be provided in the same order as the target_cause_ids.
    - project (str): Your team's cluster project. split_cod_model launches cluster jobs which will run under this project.
    - gbd_round_id (int, optional): The gbd_round_id for models being split. Defaults to current gbd round.
    - decomp_step (str, optional): The decomposition step used to query the appropriate source model version for splitting. If no value provided for target_meid_decomp_steps, will also be used to find proportion model versions. Defaults to None, will raise an error if not a valid decomp_step argument for the given GBD round.
    - output_dir (str, optional): Place where you want new draws to be saved. Defaults to "FILEPATH/{given gbd_round}".
    - target_meid_decomp_steps (Union[List[str], str], optional): Users may want target me_ids from a decomp_step different from their cod model's. target_meid_decomp_steps tells split_cod_model where its target proportion me_ids are saved in decomp. Defaults to None, where split_cod_model uses the plain decomp_step argument for both the target_meids and the source_cause_id. Can be a list of values that positionally map to the list of target_meids. Can also be a single-string decomp_step, where split_cod_model will look for all proportion models in that decomp_step.

Returns:
    Dataframe containing the output directory where either the draws or an errors logfile can be found.


Resource Requirements
*****************

Split model requires quite a bit of cluster resources to run. We strongly recommend requesting **30 threads and 60 GB of memory**.


Example usage
*****************

Here's an example of calling split_cod_model on maternal causes and MEs where we use proprtion models from different decomp_steps:

Python:

.. code::

    In [1]: from split_models.split_cod import split_cod_model
    In [2]: source_id = 366
    In [3]: target_ids = [367, 368, 369, 370, 371, 375, 379, 376]
    In [4]: target_meids = [2526, 2527, 2528, 2529, 2530, 2606, 2531, 2519]
    In [5]: target_meid_decomp_steps = [
       ...:     'iterative', 'iterative', 'iterative', 'release2'
       ...:     'release2', 'iterative', 'iterative', 'iterative']
    In [6]: project='proj_example'
    In [7]: df = split_cod_model(
       ...:     source_cause_id=source_id,
       ...:     target_cause_ids=target_ids,
       ...:     target_meids=target_meids,
       ...:     project=project,
       ...:     gbd_round_id=6,
       ...:     decomp_step='step1',
       ...:     target_meid_decomp_steps=target_meid_decomp_steps)


R:

.. code::

    > source("FILEPATH/split_cod_model.R")

    > source_id <- 366

    > target_ids <- c(367, 368, 369, 370, 371, 375, 379, 376)

    > target_meids <- c(2526, 2527, 2528, 2529, 2530, 2606, 2531, 2519)

    > project <- 'proj_example'

    > data <- split_cod_model(source_cause_id=source_id,
                              target_cause_ids=target_ids,
                              target_meids=target_meids,
                              project=project,
                              gbd_round_id=6,
                              decomp_step="step1",
                              target_meid_decomp_steps="iterative")


Stata:

.. code::

    . run "FILEPATH/split_cod_model.ado"

    . local source_id 366

    . local target_ids 367 368 369 370 371 375 379 376

    . local target_meids 2526 2527 2528 2529 2530 2606 2531 2519

    . local project "proj_example"

    . split_cod_model, source_cause_id(`source_id') target_cause_ids(`target_ids') target_meids(`target_meids') project(`project') gbd_round_id(6) decomp_step(step1) clear

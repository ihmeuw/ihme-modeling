## CMS Processing Pipeline

The purpose of the CMS processing pipeline is to turn claims level ICD-mart data into aggregated bundle level disease estimates.
A row in the ICD-mart data should represent a unique combination of a claim and a diagnosis.
A row in the final processed data is the transformation of those close claims, de-duplicated and summed across demographics as appropriate divided by the corresponding sample_size of each demographic group.

The majority of the processing code for the CMS data processing pipeline lives in this directory.

### Intro
api.internal.CreateCmsEstimates is the interface for the CMS pipeline. You can either call
this function as a one-off for testing/development/special requests/etc or you can call it in
a swarm, with hundreds or thousands of unique argument values, depending on your requirements.
This interface will import most modules in the pipeline/lib directory where these modules roughly map to specific processing steps like assigning age or merging on sample size.

### Creating CMS estimates for a single bundle, estimate, CMS system and deliverable
Review the docstring of api/internal.py for additional info, but currently the required arguments are
 - cms_system: Create estimates from Medicare or Medicaid Data using `"mdcr"` or `"max"`.
 - run_id: Instantiated Clinical `run_id` in `DATABASE`
 - bundle_id: Standard clinical `bundle_id`. Must be present in the current clinical map.
 - estimate_id: Standard clinical `estimate_id`

By default, the pipeline will create the `"gbd"` deliverable.

 Upon instantiation the class will create a unique id (technically a uuid.uuid4 from Python) which acts as a versioning system for each specific instance of the class. Logs, plots, and intermediate data are stored on LU_CMS using this uuid in the following way:
 ```
 FILEPATH

 Actual example from a production run:
 FILEPATH
```
If the `write_intermediate_data` argument is `True`, which it is not by default, then the class will write both plots and data to the directory above.
If the `write_final_data` argument is `True` then a `run_id` must also be provided because the function writes final data only within a run id directory. The final data is stored within the `self.df` attribute and can be accessed or written to an alternative directory using it.
Please Note: Identifiable data cannot be moved from the `FILEPATH` dir. This data is marked by the `is_identifiable` column equal to `1`. Currently the only sub-routine that will remove the identifiable flag (eg set it to 0) is noise reduction.

### Swarm
A CMS swarm is essentially a collection of arguments for the class outlined above. Each row in a swarm should have a unique combination of bundle, estimate, cms system and deliverable values. Any duplication will result in final data being overwritten and potentially a race condition.

An example of a full swarm workflow example is available at `./cms_pipeline_example.md`


### Running the Pipeline

The requirements to run the CMS pipeline by creating a swarm are as follows:

***If a new map is required or a major change has been implemented to the intermediate tables***:
1. Run the [workflow of workflows](URL) to make all intermediate tables, or make each table individually followed by creating the ICD-Mart using [this module](URL)


***If a new map or methods update is NOT required the processing workflow can begin here***:

2. Initialize a new run using [ClaimsWrappers.initialize_run_metadata](URL) and create the corresponding run directories in `FILEPATH` and `FILEPATH`.
3. If the swarm methods have undergone major changes, use the `BuildYaml` class to create a new workflow parameter map. Otherwise use the `FromBuild` class to replicate the existing parameter map with any needed updates.
4. To submit the swarm and run the workflow, pass a run_id to `launch_cms_pipeline.cms_pipeline`.
5. Upload GBD data to the clinical database using the `ClaimsUploader` subclass in [Uploader](URL)

Notes on Runnning Swarms
- Swarm task run-time distribution is very dispersed. 95% of jobs will finish in less than 30 minutes, but some large incidence jobs will take hours to run. The main reason for this is our "recursive_duration" function
- The jobmom workflow is incredibly simple. The DAG has a single layer to it and all jobs are run in that layer. There are no dependencies or additional steps

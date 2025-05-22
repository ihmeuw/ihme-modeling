# Clinical Configuration Files
- `manager.py` : Code to create and modify configuration files.
- `parser.py` : YAML parser and parameter cache.
- `testing` : Directory contianing e2e and unit tests for `manager.py` and `parser.py`.

<br>

# Build a YAML Configuration File for a New Workflow.

```python
from inpatient.Clinical_Runs.utils.config import manager

# Define callables in the workflow with full python import path.
funcs = ["marketscan.src.pipeline.lib.jobmon.submit_jobmon.submit_marketscan",
         "cms.src.pipeline.lib.create_new_cms_swarm.create_new_swarm",
         "pol_nhf.src.pipeline.lib.swarm.submit_swarm"]

# Instantiate with an output directory and name to give config file.
mb = manager.BuildYaml(out_dir=FILEPATH, workflowname="major_claims_run")

# Build a workflow configuration template with default parameter values.
mb.template(callables=funcs, title="Claims Run: Marketscan, CMS, Poland", type_hints=False)

# Update any parameters from their default values.
mb.param("run_plotting", True)
mb.param("estimate_ids", [17, 21])
mb.param("deliverables", ["gbd", "correction_factors"])
mb.param("clinical_age_group_set_id", 3)
mb.param("write_intermediate_data", True)
mb.param("bundles_to_run", [3, 74])
mb.param("run_id", 900)
mb.param("write_final_data", True)
mb.param("cms_systems", ["mdcr", "max"])

# Make and write the configuration file with any updated parameter values.
mb.make()
```

Configuration file is now available.

<br>

# Use an Existing YAML Configuration File.

To use and existing configuration file for a subsequent run, there are two distinct use cases:
1.  Replicate a workflow by copying the previous run parameters.
2.  Replicate a workflow with a modification to parameter value(s).

<br>

```python
from inpatient.Clinical_Runs.utils.config import manager

mf = manager.FromBuild(prior_build_path=FILEPATH, out_dir=FILEPATH)
```
### To copy all parameters with updated build metadata to `out_dir`:
This replicates the prior build and retains all previous parameter value(s) and saves to the designated output directory. Build metadata will be updated.

```python
mf.replicate()
```
### To change any parameter value(s) and update the build:
This will update any specified parameter value(s) and reuse any other existing values. By default, any updated parameter must have the same base datatype as the previously used value.  For example, is `parameter1` was used originally associated to an integer value, if modifying `parameter1`, the new value must also be an integer or able to be cast to one (will attempt to cast). If the parameter value needs to change datatypes, this enforcement can be turned off by passing `strict=False` into `replicate()`.
```python
# Look at the current parameters, values, and where they map.
mf.view_params()

# Fill new parameters into dictionary, keyed by parameter name.
# Only need to add parameters that are changing.
new_params: Dict[str, Any] = {...}
mf.replicate(new_param_map=new_params)

# View old/new parameters, values, and where they map.
mf.view_params()
```

If `out_dir` is not specified, the new configuration yaml file will be written to the same location as the old configuration yaml. The previous configuration will not be overwritten, but marked with the prefix `depr_` to the filename.

<br>

# Parse a Configuration File.

```python
from clinical_info.Clinical_Runs.utils.config.parser import parser

config = parser(path=FILEPATH)
```

Each set of parameters and values will be available as an attribute in `config` named after the fuction/class they are associated with. Additionally, `config.info` will contain information such as configuration metadata and detailed workflow steps.

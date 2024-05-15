Risk factors pipeline code
Includes code for both SEVs and PAFs/scalars

# Generalized ensemble model (GenEM)

```
arc_main.py
Forecasts an entity using the ARC method
```

```
collect_submodels.py
Collects and collapses components into genem for future stage
```

```
constants.py
FHS pipeline for risk factors local constants
```

```
create_stage.py
Create tasks for GenEM
```

```
get_model_weights_from_holdouts.py
Collects submodel predictive validity statistics to compile sampling weights for genem
```

```
model_restrictions.py
Captures restrictions in which models get run for each entity/location
```

```
predictive_validity.py
Calculates the RMSE between forecast and holdouts across location & sex
```

```
run_stagewise_mrbrt.py
Forecasts entities using MRBRT (Meta-regression, Bayesian, regularized, trimmed) model
```

# Population attributable fractions (PAFs)

Note: We first compute all the cause-risk-specific PAFs using `compute_pafs.py`,
followed by the cause-only PAFs and scalars using `compute_scalar.py`

```
compute_paf.py
Compute and export all the cause-risk-pair PAFs for given acause
```

```
compute_scalar.py
Computes aggregated acause specific PAFs and scalars
```

```
constants.py
FHS pipeline scalars local constants
```

```
forecasting_db.py
Functions related to PAF queries
```

```
utils.py
Utility/DB functions for the scalars pipeline
```

# Severity exposure values (SEVs)

Note: the SEV pipeline consists of five primary stages:
1. Compute past intrinsic SEVs (stored in past_sev/risk_acause_specific/). See sev/compute_past_intrinsic_sev.py
2. (a) PV run on past years, and (b) full-draws forecast. 
3. Export sampling weights based on PV statisics.
4. Collect and concat forecast draws based on PV statistics.
5. Compute future intrinsic SEVs. Stages 2-5 are contained in GenEM.

```
compute_future_mediator_total_sev.py
Compute cause-risk-specific future total SEV, given acause and risk
```

```
compute_past_intrinsic_sev.py
Compute Intrinsic SEV of a mediator
```

```
constants.py
FHS pipeline SEVs local constants
```

```
mediation.py
Functions for understanding the mediation hierarchy
```

```
rrmax.py
A wrapper around the central read_rrmax, that handles the PAFs of 1 case
```

```
run_workflow.py
Construct and execute SEV workflow
```
Mortality pipeline code

The primary workflow for cause-specific computation begins with `run_cod_model.main()`
as the main entry point, followed by `y_star`, then `sum_to_all_cause`, then `squeeze`.

# Data transformation

```
correlate.py
Correlate residual error and modeled results
```

```
exponentiate_draws.py
Exponentiate a distribution, preserving the mean
```

```
intercept_shift.py
various implementations of the intercept shift operation
```

# Lib

```
config_dataclasses.py
Dataclasses for capturing the configuration of the pipeline
```

```
downloaders.py
Functions used by run_cod_model.py to import past data
```

```
get_fatal_causes.py
Get the fatal cause dataframe for the input 
```

```
intercept_shift.py
Load past data and use it to apply an intercept shift to preds at the draw level
```

```
intercept_shift.py
Load past data and use it to apply an intercept shift to preds at the draw level
```

```
make_all_cause.py
Makes a new version of mortality that consolidates data from externally and internally modeled causes
```

```
make_hierarchies.py
Functions used by Mortality stage 2 and stage 3 to set up the aggregation hierarchy and the cause set for ARIMA to operate on
```

```
mortality_approximation.py
Perform mortality approximation for every cause
```

```
run_cod_model.py
Functions to run cause of death model
```

```
smoothing.py
Utilities related to smoothing in the latent trend model
```

```
squeeze.py
Module for squeezing results into an envelope
```

```
sum_to_all_cause.py
Aggregates the expected value of mortality or ylds up the cause hierarchy and computes y_hat and y_past
```

```
y_star.py
Computes y-star, which is the sum of the latent trend component and the y-hat predictions from the GK model
```


# Models
```
GKModel.py
Contains an implementation of the Girosi-King ("GK") Model
```

```
model_parameters.py
Contains a class that encapsulates logic/info related GK model parameters
```

```
omega.py
Contains utilities for preparing the Bayesian omega priors of the GK model
```

```
post_process.py
Contains utilities for processing the GK model output into a more usable form
```

```
pre_process.py
contains utilities for preparing the GK model input
```

```
pre_process.py
contains utilities for preparing the GK model input
```

```
pooled_random_walk.py
contains tools creating a collection of correlated random walk projections
```

```
random_walk.py
Contains the random walk model
```

```
remove_drift.py
Contains functions for attenuating or removing the drift effect from epsilon
```
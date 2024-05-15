Nonfatal pipeline code

The `run_model.one_cause_main()` function is the main entry point to computation.

# Lib

```
check_entity_files.py
Checks that the proper entity files are output by a given job
```

```
constants.py
Nonfatal pipeline local constants
```

```
indicator_from_ratio.py
Computes target indicator from existing ratio and indicator data
```

```
model_parameters.py
Parameters to be used for model strategy
```

```
model_strategy.py
Where nonfatal modeling strategies and their parameters are managed/defined
```

```
model_strategy_queries.py
Has query functions that give nonfatal modeling strategies and their params
```

```
ratio_from_indicators.py
Computes ratio of two indicators for past data
```

```
run_model.py
Script that forecasts nonfatal measures of health
```

```
yld_from_prevalence.py
Computes and saves YLDs using prevalence forecasts and average disability weight.
```


# Models
```
arc_method.py
Module with functions for making forecast scenarios
```

```
limetr.py
Provides an interface to the LimeTr model
```

```
omega_selection_strategy.py
Strategies for determining the weight for the Annualized Rate-of-Change (ARC) method
```

```
processing.py
Contains all the functions for processing data for use in modeling
```

```
validate.py
Functions related to validating inputs, and outputs of nonfatal pipeline
```
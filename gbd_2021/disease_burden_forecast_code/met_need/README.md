Pipeline code for met need for modern contraceptive use
To model met need, we use an ARC-only run of our GenEM (Generalized Ensemble Model) pipeline.
This consists of iterated parallelized calls to the arc_all_omegas function in arc_main.py,
in this case specifically with entity="met_need" and stage="met_need"

```
arc_main.py
Forecasts an entity using the Annualized Rate-of-Change (ARC) method.
```

```
arc_method.py
ARC method module with functions for making forecast scenarios
```

```
collect_submodels.py
Script to collect and collapse components into genem for future stage
```

```
constants.py
FHS generalized ensemble model pipeline for forecasting - local constants
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
omega_selection_strategies.py
Strategies for determining the weight for the ARC method
```

```
predictive_validity.py
Utility functions for determining predictive validity
```


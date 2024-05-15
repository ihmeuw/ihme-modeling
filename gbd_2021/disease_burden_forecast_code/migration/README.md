# Migration pipeline code

The general overall order for running these scripts is:
1. aggregate_shocks_and_sdi.py
2. model_migration.py
3. run_model.py
4. csv_to_xr.py
5. arima_and_generate_draws.py
6. migration_rate_to_count.py
7. age_sex_split.py
8. balance_migration.py

```
age_sex_split.py
Splits the migration into separate age-sex groups
```

```
aggregate_shocks_and_sdi.py
Produce aggregate versions of shocks and reshape mean sdi for use in modeling migration
```

```
arima_and_generate_draws.py
Applies random walk on every-5-year migration data without draws
```

```
balance_migration.py
Combines the separate location files from the age-sex splitting of migration
```

```
csv_to_xr.py
Converts .CSV predictions to xarray file and makes epsilon
```

```
migration_rate_to_count.py
Converts migration rates output by draw generation step to counts for use in age-sex splitting step
```

```
model_migration.py
Cleans and models UN migration estimates for forecasting
```

```
model_strategy.py
Where migration modeling strategies and their parameters are managed/defined
```

```
model_strategy_queries.py
Has query functions that give nonfatal modeling strategies and their parameters
```

```
run_model.py
Forecasts migration with LimeTr
```
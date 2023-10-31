# covid-historical-model
IHME empirical model of past COVID-19 infections, confirmed cases, hospitalizations, and deaths.

```
git clone https://github.com/ihmeuw/covid-historical-model.git
cd covid-historical-model
make install_env ENV_NAME='rates-pipeline'
conda activate rates-pipeline
make clean
rates_pipeline --help
```

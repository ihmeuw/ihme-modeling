[![PyPI](https://img.shields.io/pypi/v/pydisagg)](https://pypi.org/project/pydisagg/)
![Python](https://img.shields.io/badge/python-3.8,_3.9,_3.10,_3.11,_3.12-blue.svg)
[![Downloads](https://static.pepy.tech/badge/pydisagg)](https://pepy.tech/project/pydisagg)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/ihmeuw-msca/pyDisagg/build.yml)
[![GitHub](https://img.shields.io/github/license/ihmeuw-msca/pyDisagg)](./LICENSE)
[![codecov](https://codecov.io/gh/ihmeuw-msca/pyDisagg/graph/badge.svg?token=OCIUF5DW0X)](https://codecov.io/gh/ihmeuw-msca/pyDisagg)
[![DOI](https://zenodo.org/badge/576042829.svg)](https://doi.org/10.5281/zenodo.14641581)

## Dissaggregation under Generalized Proportionality Assumptions
This package disaggregates an estimated count observation into buckets based on the assumption that the rate (in a suitably transformed space) is proportional to some baseline rate. 

The most basic functionality is to perform disaggregation under the rate multiplicative model that is currently in use. 

The setup is as follows: 

Let $D$ be an aggregated measurement across groups $1,\ldots,k$, where the population of each is $p_i,\ldots,p_k$. Let $f_1,\ldots,f_k$ be the baseline pattern of the rates across groups, which could have potentially been estimated on a larger dataset or a population in which have higher quality data on. Using this data, we generate estimates for $D_i$, the number of events in group $g_i$ and $\hat{f_{i}}$, the rate in each group in the population of interest by combining $D$ with $f_1,...,f_k$ to make the estimates self consistent. 

Mathematically, in the simpler rate multiplicative model, we find $\beta$ such that

$$D = \sum_{i=1}^{k}\hat{f}_i \cdot p_i $$

Where

$$\hat{f_i} = T^{-1}(\beta + T(f_i)) $$


This yields the estimates for the per-group event count, 

$$D_i = \hat f_i \cdot p_i $$

For the current models in use, T is just a logarithm, and this assumes that each rate is some constant muliplied by the overall rate pattern level. Allowing a more general transformation T, such as a log-odds transformation, assumes multiplicativity in the associated odds, rather than the rate, and can produce better estimates statistically (potentially being a more realistic assumption in some cases) and practically, restricting the estimated rates to lie within a reasonable interval. 

## Current Package Capabilities and Models
Currently, the multiplicative-in-rate model RateMultiplicativeModel with $T(x)=\log(x)$ and the Log Modified Odds model LMOModel(m) with $T(x)=\log(\frac{x}{1-x^{m}})$ are implemented. Note that the LMOModel with m=1 gives a multiplicative in odds model.

A useful (but slightly wrong) analogy is that the multiplicative-in-rate is to the multiplicative-in-odds model as ordinary least squares is to logistic regression in terms of the relationship between covariates and output (not in terms of anything like the likelihood)

Increasing m in the model LMOModel(m) gives results that are more similar to the multiplicative-in-rate model currently in use, while preserving the property that rate estimates are bounded by 1. 

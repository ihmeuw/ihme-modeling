---
title: 'pyDisagg: A Python Package for Data Disaggregation'

tags:
  - Python
  - Age/Sex Splitting
  - Inverse Problems
  - Uncertainty Propagation 

authors:
  - name: Alexander Hsu
    orcid: 0000-0002-8198-945X
    affiliation: 1,2
  - name: Sameer Ali 
    orcid: 0009-0009-0141-601X
    affiliation: 1
  - name: Peng Zheng
    orcid: 0000-0003-3313-215X
    affiliation: 1
  - name: Kelsey Maass 
    orcid: 0000-0002-9534-8901
    affiliation: 1    
  - name: Aleksandr Aravkin
    orcid: 0000-0002-1875-1801
    affiliation: "1, 2"

affiliations:
 - name: Department of Health Metrics Sciences, University of Washington
   index: 1
 - name: Department of Applied Mathematics, University of Washington
   index: 2

date: 02.23.2024
bibliography: paper.bib

---

# Summary

Data sources  report aggregated data for many reasons. Aggregating data may assuage privacy concerns, may be cheaper to tabulate, and easier to release. In global health applications, many data sources report data that is aggregated across age (for example, all-age prevalence of a disease), across sex (both-sex incidence of a disease), and across location (national estimates of mortality from a particular disease). 

When building processing workflows for analyzing and modeling the data, the user must either incorporate the aggregation mechanism into the model, or somehow disaggregate the data. While the former option is feasible in some contexts, the latter option significantly simplifies processing and modeling in large-scale analyses. 
Many data sources used by the Institute for Health Metrics and Evaluation report aggregated data that is split, for example 
into age-specific and sex-specific bins, for futher processing, using additional information. 

The `pyDisagg` package implements simple methods for data splitting into any set of categories. Given 
- An aggregated observation and uncertainty (for example, all-age prevalence with associated uncertainty)
- Set of categories that were `aggregated' by the datapoint (e.g. which age categories were aggregated to get the prevalence)
- Frequency pattern most relevant to the datapoint (e.g. age distribution of study or location relevant to the datapoint)
- Global pattern for observable of interest (e.g. global prevalence by age)
the `pyDisagg` package produces split estimates into the specified bins for further processing. 

# Statement of Need

Nearly all groups within the Institute of Health Metrics and Evaluation (IHME) currently use some form of splitting, with age and sex-splitting being the most common. 
Typical assumptions are that 
- split datapoints should follow the global pattern up to a multiplicative constant
- uncertainty is propagated by draws, i.e. performing the computation for different realizations of the datapoint and the global pattern (if uncertain).

The `pyDisagg` package provides this functionality, along with additional technical solutions that allow
- splitting of fundamentally bounded quantities, such as prevalence, which has to lie between 0 and 1
- allowing draw-free uncertainty propagation using the multivariate delta method
- guarantees that uncertainty estimates are consistent with bounds for bounded quantities


# Core idea and structure of `pyDisagg`

Let $D_{1,...,k}$ be an aggregated measurement across groups ${g_1,...,g_k}$, where the population of each is $p_i,...,p_k$. Let $f_1,...,f_k$ be the baseline pattern of the rates across groups, potentially available with uncertainty. Using these data, we generate estimates for $D_i$, the number of events in group $g_i$ and $\hat{f_{i}}$, the rate in each group in the population of interest by combining $D_{1,...,k}$ with $f_1,...,f_k$ to make the estimates self consistent. 

Mathematically, in the simpler rate multiplicative model, we find $\beta$ such that 
$$D_{1,...,k} = \sum_{i=1}^{k}\hat{f}_i \cdot p_i $$
where
$$\hat{f_i} = T^{-1}(\beta + T(f_i)) $$
This yields the estimates for the per-group event count, 
$$D_i = \hat f_i \cdot p_i $$

When 
$$T(x) = \log(x),$$
the log transform, each rate is a constant muliplied by the overall rate pattern level. 

When 
$$T(x) = \log(x/1-x),$$
the log-odds transform, multiplicativity is assumed in the associated odds, rather than the rate, and restricts the estimated rates to lie within a reasonable interval. 

## Current Package Capabilities and Models
Currently, the multiplicative-in-rate model RateMultiplicativeModel with $T(x)=\log(x)$ and the Log Modified Odds model with $T(x)=\log(\frac{x}{1-x^{m}})$ are implemented. 
The LMO_model with m=1 gives the multiplicative odds model described above. Higer $m$ give results that are more similar to the multiplicative-in-rate model, while preserving the property that rate estimates are bounded by 1. 

## Uncertainty Propagation
The `pyDisagg` packages uses the multivariate delta method to propagate unceratinty. Given a variance for the observation and for the global rate, the package produces asymptotically valid 
uncertainty intervals for the multiplicative factors within the transforms $T$. The confidence intervals for the factors are then used to obtain confidence intervals for the split datapoints using the transforms, which guarantees that final estimates respect the bounds imposed by the transforms. 

# Ongoing Research and Dissemination

Age- and sex- splitting is currently widely used to support ongoing work for the Global Burden of Disease (GBD) study. For example, the GBD capstones on Diseases and Injuries [@vos2020global], 
Risk Factors [@murray2020global], and AntiMicrobial Resistance [@murray2022global] all heavily use age- and sex-splitting. 

# References

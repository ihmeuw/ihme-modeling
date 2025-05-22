PyDisagg: Dissaggregation under Generalized Proportionality Assumptions
=======================================================================

|PyPI| |Python| |GitHub Workflow Status| |GitHub|

This package disaggregates an estimated count observation into buckets
based on the assumption that the rate (in a suitably transformed space)
is proportional to some baseline rate.

The most basic functionality is to perform disaggregation under the rate
multiplicative model that is currently in use.

The setup is as follows:

Let :math:`D_{1,...,k}` be an aggregated measurement across groups
:math:`{g_1,...,g_k}`, where the population of each is
:math:`p_i,...,p_k`. Let :math:`f_1,...,f_k` be the baseline pattern of
the rates across groups, which could have potentially been estimated on
a larger dataset or a population in which have higher quality data on.
Using this data, we generate estimates for :math:`D_i`, the number of
events in group :math:`g_i` and :math:`\hat{f_{i}}`, the rate in each
group in the population of interest by combining :math:`D_{1,...,k}`
with :math:`f_1,...,f_k` to make the estimates self consistent.

Mathematically, in the simpler rate multiplicative model, we find
:math:`\beta` such that

.. math:: D_{1,...,k} = \sum_{i=1}^{k}\hat{f}_i \cdot p_i 

Where

.. math:: \hat{f_i} = T^{-1}(\beta + T(f_i)) 

This yields the estimates for the per-group event count,

.. math:: D_i = \hat f_i \cdot p_i 

For the current models in use, T is just a logarithm, and this assumes
that each rate is some constant muliplied by the overall rate pattern
level. Allowing a more general transformation T, such as a log-odds
transformation, assumes multiplicativity in the associated odds, rather
than the rate, and can produce better estimates statistically
(potentially being a more realistic assumption in some cases) and
practically, restricting the estimated rates to lie within a reasonable
interval.

Current Package Capabilities and Models
---------------------------------------

Currently, the multiplicative-in-rate model RateMultiplicativeModel with
:math:`T(x)=\log(x)` and the Log Modified Odds model LMO_model(m) with
:math:`T(x)=\log(\frac{x}{1-x^{m}})` are implemented. Note that the
LMO_model with m=1 gives a multiplicative in odds model.

A useful (but slightly wrong) analogy is that the multiplicative-in-rate
is to the multiplicative-in-odds model as ordinary least squares is to
logistic regression in terms of the relationship between covariates and
output (not in terms of anything like the likelihood)

Increasing m in the model LMO_model(m) gives results that are more
similar to the multiplicative-in-rate model currently in use, while
preserving the property that rate estimates are bounded by 1.

.. |PyPI| image:: https://img.shields.io/pypi/v/pydisagg
   :target: https://pypi.org/project/pydisagg/
   :height: 20px
.. |Python| image:: https://img.shields.io/badge/python-3.8,_3.9-blue.svg
   :height: 20px
.. |GitHub Workflow Status| image:: https://img.shields.io/github/actions/workflow/status/ihmeuw-msca/pyDisagg/build.yml
   :height: 20px
.. |GitHub| image:: https://img.shields.io/github/license/ihmeuw-msca/pyDisagg
   :target: ./LICENSE
   :height: 20px


---------------------------------------

.. toctree::
   :hidden:

   getting_started/index
   user_guide/index
   .. api_reference/index
   .. developer_guide/index


.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - :ref:`Getting started`
     - :ref:`User guide`

   * - If you are new to pydisagg, this is where you should go. It contains how to install the package, and how to run it in Python or R,
     - The user guide provides in-depth information on key concepts of package building with useful background information and explanation.

.. .. list-table::
..    :header-rows: 1
..    :widths: 50 50

..    * - :ref:`API reference`
..      - :ref:`Developer guide`
   
..    * - If you are looking for information on a specific module, function, class or method, this part of the documentation is for you.
..      - Want to improve the existing functionalities and documentation? The contributing guidelines will guide you through the process.

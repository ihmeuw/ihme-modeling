.. _rr1_binary:

=======================
Relative Risk 1: Binary
=======================

Relative risk (RR) is the most common measurement type for the applications of ``MRTool``.
Here we take a chance to introduce the basic concepts regarding relative risk, and
how we build different types of relative risk models in ``MRTool``.

Relative risk is the probability ratio of a certain outcome between exposed and unexposed group.
For more information please check the `wiki page <https://en.wikipedia.org/wiki/Relative_risk>`_.
Here we use smoking and lung cancer as a risk-outcome pair to explain the idea.

Imagine the experiment is conducted with two groups, smoking (e) and non-smoking (u) group.
We record the probability of getting lung cancer among the two groups, :math:`P_e`, :math:`P_u`
and the relative risk can be expressed as,

.. math::

   RR = \frac{P_e}{P_u}.

To implement meta-analysis on the effect of smoking, we often convert the collected relative risks from different
studies (`longitudinal <https://en.wikipedia.org/wiki/Longitudinal_study>`_ or not) to log space,
for the convenience of removing the sign restriction,

.. math::

   \ln(RR) = \ln(P_e) - \ln(P_u).

To setup the binary model, we simply parametrize the log relative risk with an intercept,

.. math::

   \ln(RR) = \mathbf{1} (\beta + u),

where :math:`\beta` is the fixed effect for intercept and :math:`u` is the random effect.
When :math:`\beta` is `significantly <https://en.wikipedia.org/wiki/Statistical_significance>`_
greater than zero, we say that it is harmful.
For other risk outcome pair, there is possibility that :math:`\beta` is significantly less than zero,
in which case we will call it protective.

Very often instead of only considering smoking vs non-smoking (binary), we also want to study the effects
under different exposure to smoking. The most common assumption is log linear, please check
:ref:`rr2_log_linear` for the details.



Sample Code
-----------

To setup the problem, we will only need ``LinearCovModel``.

.. code-block:: python

   from mrtool import MRData, LinearCovModel, MRBRT

   data = MRData()
   # `intercept` is automatically added to the data
   # no need to pass it in `col_covs`
   data.load_df(
       df=df,
       col_obs='ln_rr',
       col_obs_se='ln_rr_se',
       col_study_id='study_id'
   )
   cov_model = LinearCovModel('intercept', use_re=True)
   model = MRBRT(data, cov_models=[cov_model])

.. _rr2_log_linear:

===========================
Relative Risk 2: Log Linear
===========================

When analyzing relative risk across different exposure levels,
the most widely used assumption is that the model is log linear.
We parametrize the log risk as a linear function of exposure,

.. math::

   \ln(RR) = \ln(R_e) - \ln(R_u) = x_a (\beta + u) - x_r (\beta + u) = (x_a - x_r)(\beta + u),

where :math:`x` is the exposure, :math:`\beta`, :math:`u` are the fixed and random effects,
and :math:`a`, :math:`r` refer to "alternative" and "reference" groups.
They are consistent with previous notation, "exposed" and "unexposed".

**Remark 1**: **No intercept!**

Notice that in this model, we do NOT include the intercept to model the log risk.
It is not possible to infer the absolute position of the risk curve using relative risk data,
only the relative position.

To see this, first assume that we have intercept in the log risk formulation,
:math:`\ln(R) = (\beta_0 + u_0) + x (\beta_1 + u_1)`,
when we construct the log relative risk,

.. math::

   \begin{aligned}
   \ln(RR) =& \ln(R_e) - \ln(R_u) \\
   =& (\beta_0 + u_0) + x_a (\beta_1 + u_1) - ((\beta_0 + u_0) + x_r (\beta_1 + u_1)) \\
   =& (x_a - x_r)(\beta_1 + u_1)
   \end{aligned}

the intercept cancels and we returns to the original formula.

**Remark 2**: **No intercept! Again!**

The other possible use of the intercept is to directly model
the log relative risk, instead of log risk,

.. math::

   \ln(RR) = (\beta_0 + u_0) + (x_a - x_r)(\beta_1 + u_1).

This does NOT work due to the fact that when :math:`x_a` is equal to :math:`x_r`,
we expect the log relative risk is zero.

Compare to :ref:`rr1_binary`, where we use the intercept to model the log relative risk,

* In the binary model, we directly model the log relative risk instead of log risk.
* In the binary model, we never have the case when the exposures for two groups are the same.


Sample Code
-----------

To setup the problem, we will only need ``LinearCovModel``, just as in :ref:`rr1_binary`.

If there is already a column in the data frame corresponding to the exposure differences,
we can simply use it as the covariate.

.. code-block:: python

   from mrtool import MRData, LinearCovModel, MRBRT

   data = MRData()
   data.load_df(
       df=df,
       col_obs='ln_rr',
       col_obs_se='ln_rr_se',
       col_covs=['exposure_diff']
       col_study_id='study_id'
   )
   cov_model = LinearCovModel('exposure_diff', use_re=True)
   model = MRBRT(data, cov_models=[cov_model])

Otherwise if you pass in the exposure for the "alternative" and "reference" group,
the ``LinearCovModel`` will setup the model for you.

.. code-block:: python

   data.load_df(
       df=df,
       col_obs='ln_rr',
       col_obs_se='ln_rr_se',
       col_covs=['exposure_alt', 'exposure_ref']
       col_study_id='study_id'
   )
   cov_model = LinearCovModel(alt_cov='exposure_alt', ref_cov='exposure_ref', use_re=True)

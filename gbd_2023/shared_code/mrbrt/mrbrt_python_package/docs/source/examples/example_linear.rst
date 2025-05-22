.. _example_linear:

============================
Example: Simple Linear Model
============================

In the following, we will go through a simple example of how to solve
a linear mixed effects model. Consider the following setup,

.. math::

   y_{ij} = (\beta_0 + u_{0i}) + x \beta_1 + \epsilon_{ij}

where :math:`y` is the measurement, :math:`x` is the covariate,  :math:`\beta_0` and :math:`\beta_1` is the fixed
effects, :math:`u_0` is the random intercept and :math:`\epsilon` is the measurement error.
And :math:`i` is index for study, :math:`j` is index for observation within study.

Assume our data frame looks like,

.. csv-table::
   :header: y, x, y_se, study_id
   :widths: 10, 10, 10, 10
   :align: center

   0.20, 0.0, 0.1, A
   0.29, 0.1, 0.1, A
   0.09, 0.2, 0.1, B
   0.14, 0.3, 0.1, C
   0.40, 0.4, 0.1, D

and our goal is to obtain the fixed effects and random effects for each study.


Create Data Object
------------------
The first step is to create a ``MRData`` object to carry the data information.

.. code-block:: python

   from mrtool import MRData

   data = MRData()
   data.load_df(
       df,
       col_obs='y',
       col_covs=['x'],
       col_obs_se='y_se',
       col_study_id='study_id'
   )

Notice that the ``MRData`` will automatically create an ``intercept`` in the covariate list.

Configure Covariate Models
--------------------------
The second step is to create covariate models.

.. code-block:: python

   from mrtool import LinearCovModel

   cov_intercept = LinearCovModel('intercept', use_re=True)
   cov_x = LinearCovModel('x')


Create Model and Fit Model
--------------------------
The third step is to create the model to group data and covariate models.
And use the optimization routine to find result.

.. code-block:: python

   from mrtool import MRBRT

   model = MRBRT(
       data,
       [cov_intercept, cov_x]
   )
   model.fit_model()

You could get the fixed effects and random effects by calling ``model.beta_soln`` and ``model.re_soln``.


Predict and Create Draws
------------------------
The last step is to predict and create draws.

.. code-block:: python

   # first create data object used for predict
   # the new data frame has to provide the same covariates as in the fitting
   data_pred = MRData()
   data_pred.load_df(
       df_pred,
       col_covs=['x']
   )

   # create point prediction
   y_pred = model.predict(data_pred)

   # sampling solutions
   beta_samples, gamma_samples = model.sample_soln(sample_size=1000)

   # create draws
   y_draws = model.create_draws(
       data_pred,
       beta_samples,
       gamma_samples
   )

Here ``y_pred`` is the point prediction and ``y_draws`` contains ``1000`` draws of the outcome.
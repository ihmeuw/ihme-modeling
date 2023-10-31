=====================
MRTool Documentation
=====================

**MRTool** (Meta-Regression Tool) package is designed to solve general meta-regression problem.
The most common features include,

* linear and log prediction function,
* spline extension for covariates,
* direct Gaussian, Uniform and Laplace prior on fixed and random effects,
* shape constraints (monotonicity and convexity) for spline.

Advanced features include,

* spline knots ensemble,
* automatic covariate selection.


Installation
------------
This package uses `data class <https://docs.python.org/3/library/dataclasses.html>`_, therefore require ``python>=3.7``.

Required packages include,

* basic scientific computing suite, Numpy, Scipy and Pandas,
* main optimization engine, `IPOPT <https://github.com/matthias-k/cyipopt>`_,
* customized packages, `LimeTr <https://github.com/zhengp0/limetr>`_ and
  `XSpline <https://github.com/zhengp0/xspline>`_,
* testing tool, Pytest.

After install the required packages, clone the repository and install MRTool.

.. code-block:: shell

   git clone https://github.com/ihmeuw-msca/MRTool.git
   cd MRTool && python setup.py install


Getting Started
---------------

To build and run a model, we only need four steps,

1. create ``MRData`` object and load data from data frame
2. configure the ``CovModel`` with covariates and priors
3. create ``MRModel`` object with data object and covriate models and fit the model
4. predict or create draws with new data and model result

In the following, we will list a set of examples to help user get familiar with
the syntax.

* :ref:`simple linear model <example_linear>`


Important Concepts
------------------

To correctly setup the model and solve problems,
it is very important to understand some key :ref:`concepts <concepts>`.
We introduce them under three categories,

* How can we match the data generating mechansim?
* How can we incorporate prior knowledge?
* How do the underlying optimization algorithms work?


.. toctree::
   :maxdepth: 2
   :hidden:

   examples/index
   concepts/index
   api_reference/index

======
MRTool
======

.. image:: https://img.shields.io/badge/License-BSD%202--Clause-orange.svg
    :target: https://opensource.org/licenses/BSD-2-Clause
    :alt: License

.. image:: https://readthedocs.org/projects/mrtool/badge/?version=latest
    :target: https://mrtool.readthedocs.io/en/latest/
    :alt: Documentation

.. image:: https://github.com/ramittal/MRTool/workflows/build/badge.svg?branch=master
    :target: https://github.com/ramittal/MRTool/actions?query=workflow%3Abuild
    :alt: BuildStatus

.. image:: https://badge.fury.io/py/MRTool.svg
    :target: https://badge.fury.io/py/mrtool
    :alt: PyPI

.. image:: https://coveralls.io/repos/github/ramittal/MRTool/badge.svg?branch=master
    :target: https://coveralls.io/github/ramittal/MRTool?branch=master
    :alt: Coverage

.. image:: https://www.codefactor.io/repository/github/ramittal/mrtool/badge/master
    :target: https://www.codefactor.io/repository/github/ramittal/mrtool/overview/master
    :alt: CodeFactor


**MRTool** (Meta-Regression Tool) package is designed to solve general meta-regression problem.
The most interesting features include,

* linear and log prediction function,
* spline extension for covariates,
* direct Gaussian, Uniform and Laplace prior on fixed and random effects,
* shape constraints (monotonicity and convexity) for spline.

Advanced features include,

* spline knots ensemble,
* automatic covariate selection.


Installation
------------

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


For more information please check the `documentation <https://mrtool.readthedocs.io/en/latest>`_.


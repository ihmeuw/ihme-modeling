.. image:: https://github.com/ihmeuw-msca/regmod/workflows/build/badge.svg
    :target: https://github.com/ihmeuw-msca/regmod/actions

.. image:: https://badge.fury.io/py/regmod.svg
    :target: https://badge.fury.io/py/regmod

Regression Models
=================

This package is design for general regression models including
generalized linear models and others.

It features

* Bayesian framework, allows user to include priors into the mdoel.
* Easy spline interface and spline shape priors and constraint.

Current model pool contains:

* Linear model
* Poisson model
* Binomial model

Install
-------
Package requires Python version above ``>=3.10``.

To install the package, the simplest way is through ``pip``,

.. code-block:: bash

    pip install regmod

Or you could clone this repository and do,

.. code-block:: bash

    python setup.py install

Acknowledgement
---------------
Design idea of this package comes from
`"Animal" package <https://github.com/ihmeuw-msca/anml>`_.
It is a general tool building template, for more detail please check the link above.

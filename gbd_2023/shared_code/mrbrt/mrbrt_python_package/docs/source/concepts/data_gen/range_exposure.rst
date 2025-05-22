.. _range_exposure:

==============
Range Exposure
==============

Very often, data is being collected over cohorts or different
groups of people, and therefore one data point can be interpreted as an average.

For example, if we are interested in the relation between smoking and relative risk
of getting lung cancer, one data point is measured by the relative risk between the smoking and the non-smoking group.
Within the smoking group, subjects have different exposures to smoking.
So what the data point measures is the average relative risk for the corresponding range of exposures.

If we denote :math:`x` as the exposure and :math:`f(x)` as the function between the outcome and exposure,
one measurement :math:`y` over a range of exposures :math:`x \in [a, b]` can be expressed as,

.. math::

   y = \frac{1}{b - a}\int_a^b f(x)\,\mathrm{d}x.

A special case is when the function :math:`f` is linear,
:math:`f(x) = \beta x`, and the expression can be simplified as,

.. math::

   y = \frac{1}{b - a}\int_a^b f(x)\,\mathrm{d}x = \frac{1}{2}(a + b) \beta.

It is equivalent to use the midpoint of the exposures as the covariate.


Sample Code
-----------

In the code, you could communicate with the program that you have a range exposure by inputting a pair of covariates
instead of one.

.. code-block:: python

   cov_model = CovModel('exposure', alt_cov=['exposure_start', 'exposure_end'])

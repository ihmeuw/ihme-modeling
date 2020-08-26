General Notes on Life Expectancy Decomposition by Cause
========================================================

The purpose of life expectancy decomposition by cause is to determine the effect of
a given cause on the difference between at-birth life expectancy for a population
at year A vs the same at year B.

The method described below represents only a general computational outline,
rather than the specific mathematics used at each step. For a more detailed
explanation of the mathematics involved see:
ADDRESS
and
ADDRESS
Each step described should be assumed to be applied to a specific location/sex combination
unless otherwise noted.

Before an at-birth life expectancy decomposition by cause can be computed for
a time period A to B, we must first compute a decomposition by age for the population
over the same time period.  Without going into specific mathematics, this is because
age is a function of time, we are measuring the difference in life
expectancy for a given time period, and thus we cannot consider time to be an independent
variable in our calculations without first separating the contribution to the difference which is due to the age structure of the given population.  The method used is encapsulated using
the Forecasting team's :py:func:`fbd_research/fdb_research/lex_decomps/andreev/delta_x`
function which implements Andreev's technique for calculating age specific
contributions to differences in life expectancy.

Once age specific decomposition has been calculated
we may perform decomposition by cause for every age group.  We pull death data
for every cause in rate space, as well as 'all cause' mortality.  The list of
causes (excluding 'all cause' which is pulled for reference) for which we pull
data must follow the general IHME standard of 'mutually exclusive and collectively
exhaustive'.  That is, no two causes should overlap in the deaths attributed to
them, and the total list of causes must represent all causes of death suffered by
the population.  This death data and the life table data decomposed by age group is
fed into the Forecasting team's
:py:func:`fdb_research/fdb_research/lex_decomps/das_gupta` function,
which uses Das Gupta's technique for the decomposition of additive functions.

This results in a by-cause decomposition of life expectancy at each age group.
The last step is to sum the decomposition over age groups to get a by cause decomposition
of at-birth life expectancy.

In order to validate our decomposition, we check that the following condition holds
true:
:math:`\sum_c x_c = (le_B - le_A)`
where :math:`x_c` is the contribution to the difference in life expectancy over the time
period A to B for cause c, and :math:`le_N` is the at-birth life expectancy at time N.

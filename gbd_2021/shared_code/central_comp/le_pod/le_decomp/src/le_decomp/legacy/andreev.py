r"""Andreev, Shkolnikov, and Begun (2002) describe a technique for
calculating age specific contributions to differences in life expectancy.
They claim that this technique is essentially equivalent to Pollard (1982) and
Arriaga (1984), in that they're all just "particular forms of a single general
algorithm that includes stepwise replacement of elements from one vector of
age-specific mortality rates by respective elements of another vector."

Given two sets of age specific all cause mortality
:math:`\vec{\mu_1}^{[1]}=(\mu_1^{[1]},...,\mu_\alpha^{[1]})` and
:math:`\vec{\mu_1}^{[2]}=(\mu_1^{[2]},...,\mu_\alpha^{[2]})`, one would like to
know how the difference in all cause mortality for age group :math:`x`
contributes to the change in life expectancy between the two populations.

To do this, Andreev et al. use a **mortality zipper** that zips together the
age specific all cause mortality at each age group, in order. This mortality
zipper is:

.. math::
    \vec{M^{[x]}} := \begin{cases}
    \mu_y^{[2]} & \text{ if } y < x\\
    \mu_y^{[1]} & \text{ if } y \ge x
    \end{cases}

Then, one may define the difference
:math:`\delta_{0|x}^{2-1} = e_0 (M^{[x]}) - e_0^1` as a contribution of ages
from :math:`y \lt x` to the overall difference :math:`e_0^2 - e_0^1`.

Taking :math:`L_{0|x} = \int_0^x l_t \ dt`, we have

.. math::
    \begin{align}
    \delta_{0|x}^{2-1} & = e_0 (M^{[x]}) \ - \ e_0^1 \\
                       & = (L_{0|x}^2 - L_{0|x}^1) \ + \ (l_x^2 - l_x^1) \
                           e_x^1 \\
    \delta_{0|x+n}^{2-1} & = (L_{0|x+n}^2 - L_{0|x+n}^1) \ + \
                           (l_{x+n}^2 - l_{x+n}^1) \ e_{x+n}^1
    \end{align}

where :math:`n` is the duration of the age group.

Then Andreev et al. define the contribution of age interval [x, x + n) to the
change in life expectancy from mortality 1 to mortality 2 to be

.. math::
    \begin{align}
    \delta_x^{2-1} & = \delta_{0|x+n}^{2-1} \ - \ \delta_{0|x}^{2-1} \\
                   & = e_0(M^{[x+n]}) \ - \ e_0(M^{[x]})
    \end{align}

One may subsitute :math:`L_{0|x} = e_0 - e_x \ l_x`
(and similarly for other :math:`L`'s) into the above equations to arrive at

.. math::
    \delta_x^{2-1} = l_x^{2} \ (e_x^{2} - e_x^{1}) \ - \ l_{x+1}^{2} \ (
                       e_{x+n}^{2} - e_{x+n}^{1}) \tag{1}

The overall difference between the two life expectancies is hence

.. math::
    e_0^2 - e_0^1 = \sum_{x=0}^{\omega} \delta_x^{2-1}

where :math:`l_{\omega + 1}` and :math:`e_{\omega + 1}` are assumed to be 0.

One can similarly compute the opposite difference

.. math::
    e_0^1 - e_0^2 = \sum_{x=0}^{\omega} \delta_x^{1-2}

by swapping the 2's and 1's in the superscripts of :math:`\delta_x^{2-1}`.
This should result in a value opposite in sign to that of
:math:`e_0^2 - e_0^1`, but not necessarily of the same magnitude, because
:math:`\delta_x^{2-1}` is somewhat different from :math:`\delta_x^{1-2}`.

To resolve this asymmetric nature of the mortality zipper, Andreev et al.
suggest averaging as a way to obtain symmetrical components:

.. math::
    \delta_x = \frac{\delta_x^{2-1} - \delta_x^{1-2}}{2} \tag{2}
"""
from collections import namedtuple
import logging

import xarray as xr


FAKE_AGE_GROUP_ID = -1


AgeMort = namedtuple("AgeMort", "age mort")


def mortality_zipper(index, mu_1, mu_2):
    """Given two sets of age specific mortality, create :math:`M^{[x]}` where x
    is the age corresponding to `index`.

    Args:
        index (int):
            The index of age_group_id in `mu_1.age_group_id.values`.
        mu_1 (xr.DataArray):
            Mortality (should be all cause) that has at least an `age_group_id`
            dimension that can be used to compute life expectancy.
        mu_2 (xr.DataArray):
            Should be identical in shape to `mu_1`, but represents a
            different population.

    Returns:
        (int, xr.DataArray):
            The age group id and the mortality zipped around that age group id.

    Raise:
        IndexError:
            If `index` is not a valid index for `mu_1.age_group_id.values`.
    """
    age_group_ids = mu_1.age_group_id.values
    if index == 0:
        mort = mu_1
        age_group_id = age_group_ids[index]
    elif index == len(age_group_ids):
        mort = mu_2
        age_group_id = FAKE_AGE_GROUP_ID
    elif 0 < index < len(age_group_ids):
        mort = xr.concat(
            [
                mu_2.sel(age_group_id=age_group_ids[:index]),
                mu_1.sel(age_group_id=age_group_ids[index:]),
            ],
            dim="age_group_id",
        )
        age_group_id = age_group_ids[index]
    else:
        raise IndexError
    return AgeMort(age_group_id, mort)


def delta_x(age_group_id, next_age_group_id, life_table_1, life_table_2):
    """Calculates age component of life expectancy decomposition given a life
    table for each population.

    Args:
        age_group_id (int):
            the age group id to calculate the life expectancy decomposition
            component for.
        next_age_group_id (int): the age group after ``age_group_id``.
        life_table_1 (xr.Dataset):
            A life table which contains an age group id dimension and data
            arrays for "ex" and "lx".
        life_table_2 (xr.Dataset):
            A life table which contains an age group id dimension and data
            arrays for "ex" and "lx".
    """
    minuend = life_table_2["lx"].sel(age_group_id=age_group_id) * (
        life_table_2["ex"].sel(age_group_id=age_group_id)
        - life_table_1["ex"].sel(age_group_id=age_group_id)
    )

    try:
        subtrahend = life_table_2["lx"].sel(age_group_id=next_age_group_id) * (
            life_table_2["ex"].sel(age_group_id=next_age_group_id)
            - life_table_1["ex"].sel(age_group_id=next_age_group_id)
        )
    except KeyError:
        err_msg = "Age group id {} is not the last valid age group id".format(
            age_group_id
        )
        assert age_group_id == life_table_1.age_group_id[-1], err_msg
        if age_group_id != life_table_1.age_group_id[-1]:
            raise ValueError(err_msg)
        subtrahend = 0
    delta_x = minuend - subtrahend
    delta_x["age_group_id"] = age_group_id
    return delta_x

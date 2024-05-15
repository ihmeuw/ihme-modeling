"""This module contains all the functions for processing data for use in modeling.

It is divided into "pre-" and "post-" processing, i.e. functions that are
called before modeling and functions that are called
after modeling.
"""

from abc import ABC
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import xarray as xr
from fhs_lib_database_interface.lib.constants import (
    AgeConstants,
    DimensionConstants,
    ScenarioConstants,
)
from fhs_lib_database_interface.lib.query import age, restrictions
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_data_transformation.lib import age_standardize, filter
from fhs_lib_data_transformation.lib.constants import GBDRoundIdConstants, ProcessingConstants
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.draws import mean_of_draws
from fhs_lib_data_transformation.lib.exponentiate_draws import bias_exp_new
from fhs_lib_data_transformation.lib.intercept_shift import (
    mean_intercept_shift,
    ordered_draw_intercept_shift,
    unordered_draw_intercept_shift,
)
from fhs_lib_data_transformation.lib.resample import resample

logger = get_logger()


class BaseProcessor(ABC):
    """A "processor" transforms data into and out of some interesting space.

    It's defined by a pair of functions, pre_process and post_process. Implementors should
    implement both. Note that the post_process function isn't a straight inverse of
    pre_process, because it takes a second argument that helps it shift the data where it
    belongs.
    """

    def pre_process(self, past_data: xr.DataArray) -> xr.DataArray:
        """Perform pre-processing: transform past_data in some way determined by the object.

        Args:
            past_data (xr.DataArray): The data to transform.

        Returns:
            The past_data, transformed.
        """
        return None

    def post_process(
        self, modeled_data: xr.DataArray, past_data: xr.DataArray
    ) -> xr.DataArray:
        """Reverse the pre-processing.

        This should operate on modeled_data and use past_data to determine an intercept-shift.

        Args:
            modeled_data (xr.DataArray):
                Data produced using model.py methods. Assumes that it contains
                forecasted years and at least years.past_end.
            past_data (xr.DataArray):
                Data to use for intercept shift. Assumes that it is in the same space produced
                by the pre_process step.

        Returns:
            modeled_data (xr.DataArray):
                The post-processed modeled_data, transformed back to normal space from
                wherever the pre_process took it.
        """
        return None


def apply_intercept_shift(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    intercept_shift: str,
    years: YearRange,
    shift_from_reference: bool,
) -> xr.DataArray:
    """Apply one of the intercept shifts, based on intercept_shift, to modeled_data.

    We "shift" modeled_data so that it aligns with past_data at the last-past year.

    Args:
        modeled_data (xr.DataArray): future data to shift. Will be aligned with the
            last-past-year data in ``past_data``.
        past_data (xr.DataArray): past data, the basis to which to shift. Should overlap with
            modeled_data in the last-past-year in ``years``.
        intercept_shift (str): the type of shift to perform: "mean", "unordered_draw", or
            "ordered_draw".
        years (YearRange): Years describing the range of years in the past_data and
            modeled_data. Those DataArrays should both contain data for the last-past-year
            (middle component).
        shift_from_reference (bool): When True, the shift is calculated once from the future
            reference scenario and applied to all other scenarios. When False, each scenario
            has its own shift applied to match past data.
    """
    if intercept_shift == "mean":
        return mean_intercept_shift(modeled_data, past_data, years, shift_from_reference)
    elif intercept_shift == "unordered_draw":
        return unordered_draw_intercept_shift(
            modeled_data, past_data, years.past_end, shift_from_reference
        )
    elif intercept_shift == "ordered_draw":
        return ordered_draw_intercept_shift(
            modeled_data,
            past_data,
            years.past_end,
            years.forecast_end,
            shift_from_reference,
        )
    else:
        raise ValueError(f"Unknown intercept shift type {intercept_shift}")


class LogProcessor(BaseProcessor):
    """A processor that transforms the data to log space and back again.

    Can also remove zero slices, take the mean, and age-standardize, depending on parameters
    to the constructor.
    """

    def __init__(
        self,
        years: YearRange,
        gbd_round_id: int,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
        remove_zero_slices: bool = False,
        no_mean: bool = False,
        bias_adjust: bool = False,
        intercept_shift: Optional[str] = None,
        age_standardize: bool = False,
        rescale_age_weights: bool = True,
        shift_from_reference: bool = True,
        tolerance: float = ProcessingConstants.DEFAULT_PRECISION,
    ) -> None:
        """Construct a LogProcessor with additional processing as specified by the args.

        Args:
            years (YearRange): Forecasting time series year range (used for age-standardize
                and intercept shifting).
            gbd_round_id (int): Which gbd_round_id the data is from (can be used in age
                standardization).
            offset (float): Optional. How much to offset the data from zero when taking the
                log.
            remove_zero_slices (bool): Optional. If True, remove slices of the data that are
                all zeros. Default False
            no_mean (bool): Optional. If True, the mean will not be taken (note that most
                model.py classes expect that the input dependent data will not have draws)
            bias_adjust (bool): Optional. Whether or not to perform bias adjustment on the
                results coming out of log space. Generally ``True`` is the recommended option
            intercept_shift (str | None): Optional. What type of intercept shift to perform,
                if any. Options are "mean", "unordered_draw", and "ordered_draw".
            age_standardize (bool): Optional. Whether to age-standardize data for modeling
                purposes (results are age-specific). If you want to have age-standardized data
                modeled and the results also be age-standardized, then you should
                age-standardize the data outside of the processor.
            rescale_age_weights (bool): Whether to rescale the age weights across available
                ages when age-standardizing.
            shift_from_reference (bool): Optional. Whether to calculate the differences used
                for the intercept shift with only the reference scenario (True), or calculate
                scenario-specific differences (False). Defaults to True.
            tolerance (float): tolerance value to supply to the "closeness" check. Defaults to
                ``ProcessingConstants.DEFAULT_PRECISION``
        """
        self.years = years
        self.offset = offset
        self.gbd_round_id = gbd_round_id
        self.remove_zero_slices = remove_zero_slices
        self.no_mean = no_mean
        self.bias_adjust = bias_adjust
        self.intercept_shift_type = intercept_shift
        self.age_standardize = age_standardize
        self.rescale_age_weights = rescale_age_weights
        self.zero_slices_dict = {}
        self.age_ratio = xr.DataArray()
        self.shift_from_reference = shift_from_reference
        self.tolerance = tolerance

    def __eq__(self, other: Any) -> bool:
        """True if the other object matches this one in a few aspects."""
        if type(self) != type(other):
            return False
        elif self.offset != other.offset:
            return False
        elif self.years != other.years:
            return False
        elif self.gbd_round_id != other.gbd_round_id:
            return False
        else:
            return True

    # Note: the docstring is given in the interface declaration, BaseProcessor.
    def pre_process(self, past_data: xr.DataArray, *args: Any) -> xr.DataArray:
        """Transform past_data into log space. (Also do several other things)."""
        if len(args) != 0:
            logger.warning(
                "Warning: passed extra args to LogProcessor.pre_process. They will be ignored."
            )

        if self.remove_zero_slices:
            past_data, self.zero_slices_dict = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )
        if not self.no_mean:
            past_data = mean_of_draws(past_data)

        past_data = log_with_offset(past_data, self.offset)

        if self.age_standardize:
            age_specific_data = past_data.copy()
            past_data = _get_weights_and_age_standardize(
                past_data, self.gbd_round_id, rescale=self.rescale_age_weights
            )
            # take the difference of age-specific and age-standardized data in last past year
            self.age_ratio = (
                age_specific_data
                - past_data.sel(age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True)
            ).sel(year_id=self.years.past_end)

        return past_data

    def post_process(
        self, modeled_data: xr.DataArray, past_data: xr.DataArray
    ) -> xr.DataArray:
        """Transform modeled_data from log into linear space. And intercept_shift if requested.

        past_data is expected to be the same as what was passed to the corresponding
        pre_process, and should still be in linear space.
        """
        if self.age_standardize:
            modeled_data = (
                modeled_data.sel(
                    age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True
                )
                + self.age_ratio
            )

        if self.remove_zero_slices:
            past_data, _ = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )

        if self.intercept_shift_type == "mean":
            past_data = mean_of_draws(past_data)

        modeled_data = invlog_with_offset(
            modeled_data, self.offset, bias_adjust=self.bias_adjust
        )

        if (
            self.intercept_shift_type == "mean"
            or self.intercept_shift_type == "unordered_draw"
            or self.intercept_shift_type == "ordered_draw"
        ):
            modeled_data = self.intercept_shift(
                modeled_data=modeled_data,
                past_data=past_data,
                offset=self.offset,
                intercept_shift=self.intercept_shift_type,
                years=self.years,
                shift_from_reference=self.shift_from_reference,
            )

        if self.zero_slices_dict:
            return _add_all_zero_slices(modeled_data, self.zero_slices_dict)

        return modeled_data

    @classmethod
    def intercept_shift(
        cls,
        modeled_data: xr.DataArray,
        past_data: xr.DataArray,
        intercept_shift: str,
        years: YearRange,
        shift_from_reference: bool,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
    ) -> xr.DataArray:
        """Move past and modeled data to log space, intercept-shift, and translate back.

        Performs the intercept-shift with the type given in ``intercept_shift``, using
        ``years`` and ``shift_from_reference`` as parameters (see docs for
        ``apply_intercept_shift``).

        Args:
            modeled_data (xr.DataArray): future data to shift. Will be aligned with the
                last-past-year data in ``past_data``.
            past_data (xr.DataArray): past data, the basis to which to shift. Should overlap
                with modeled_data in the last-past-year in ``years``.
            offset (float): Offset for the log transform (see ``log_with_offset``) and inverse
                transform.
            intercept_shift (str): Type of intercept-shift to perform. See
                ``apply_intercept_shift`` for the options.
            years (YearRange): Years describing the range of years in the past_data and
                modeled_data. Those DataArrays should both contain data for the last-past-year
                (middle component).
            shift_from_reference (bool): When True, the shift is calculated once from the
                future reference scenario and applied to all other scenarios. When False, each
                scenario has its own shift applied to match past data.
        """
        past_data = log_with_offset(past_data, offset)
        modeled_data = log_with_offset(modeled_data, offset)
        modeled_data = apply_intercept_shift(
            modeled_data,
            past_data,
            intercept_shift,
            years,
            shift_from_reference,
        )
        modeled_data = invlog_with_offset(modeled_data, offset, bias_adjust=False)
        return modeled_data


def intercept_shift_in_log(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    intercept_shift: str,
    years: YearRange,
    shift_from_reference: bool,
    offset: float = ProcessingConstants.DEFAULT_OFFSET,
) -> xr.DataArray:
    """See LogProcessor.intercept_shift."""
    return LogProcessor.intercept_shift(
        modeled_data=modeled_data,
        past_data=past_data,
        offset=offset,
        intercept_shift=intercept_shift,
        years=years,
        shift_from_reference=shift_from_reference,
    )


class LogitProcessor(BaseProcessor):
    """A Processor that transforms the data to logit space and back again.

    Can also remove zero slices, take the mean, and age-standardize, depending on parameters
    to the constructor.
    """

    def __init__(
        self,
        years: YearRange,
        gbd_round_id: int,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
        remove_zero_slices: bool = False,
        no_mean: bool = False,
        bias_adjust: bool = False,
        intercept_shift: Optional[str] = None,
        age_standardize: bool = False,
        rescale_age_weights: bool = True,
        shift_from_reference: bool = True,
        tolerance: float = ProcessingConstants.DEFAULT_PRECISION,
    ) -> None:
        """Construct a LogitProcessor with additional processing as specified by the args.

        Args:
            years (YearRange): Forecasting time series year range (used for age-standardize
                and intercept shifting).
            gbd_round_id (int): Which gbd_round_id the data is from (can be used in age
                standardization)
            offset (float): Optional. How much to offset the data from zero when taking the
                logit.
            remove_zero_slices (bool): Optional. If True, remove slices of the data that are
                all zeros. Default False
            no_mean (bool): Optional. If True, the mean will not be taken (note that most
                model.py classes expect that the input dependent data will not have draws)
            bias_adjust (bool): Optional. Whether or not to perform bias adjustment on the
                results coming out of logit space. Generally ``True`` is the recommended option
            intercept_shift (str | None): Optional. What type of intercept shift to perform,
                if any. Options are "mean", "unordered_draw", and "ordered_draw".
            age_standardize (bool): Optional. Whether to age-standardize data for modeling
                purposes (results are age-specific). If you want to have age-standardized data
                modeled and the results also be age-standardized, then you should
                age-standardize the data outside of the processor.
            rescale_age_weights (bool): Whether to rescale the age weights across available
                ages when age-standardizing.
            shift_from_reference (bool): Optional. Whether to calculate the differences used
                for the intercept shift with only the reference scenario (True), or calculate
                scenario-specific differences (False). Defaults to True.
            tolerance (float): tolerance value to supply to the "closeness" check. Defaults to
                ``ProcessingConstants.DEFAULT_PRECISION``
        """
        self.years = years
        self.offset = offset
        self.gbd_round_id = gbd_round_id
        self.remove_zero_slices = remove_zero_slices
        self.no_mean = no_mean
        self.bias_adjust = bias_adjust
        self.intercept_shift_type = intercept_shift
        self.age_standardize = age_standardize
        self.rescale_age_weights = rescale_age_weights
        self.zero_slices_dict = {}
        self.age_ratio = xr.DataArray()
        self.shift_from_reference = shift_from_reference
        self.tolerance = tolerance

    def __eq__(self, other: Any) -> bool:
        """True if the other object matches this one in a few aspects."""
        if type(self) != type(other):
            return False
        elif self.offset != other.offset:
            return False
        elif self.years != other.years:
            return False
        elif self.gbd_round_id != other.gbd_round_id:
            return False
        else:
            return True

    def pre_process(self, past_data: xr.DataArray, *args: Any) -> xr.DataArray:
        """Transform past_data into logit space. (Also do several other things)."""
        if len(args) != 0:
            logger.warning(
                "Warning: passed extra args to LogitProcessor.pre_process. "
                "They will be ignored."
            )

        if self.remove_zero_slices:
            past_data, self.zero_slices_dict = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )
        if not self.no_mean:
            past_data = mean_of_draws(past_data)

        past_data = logit_with_offset(past_data, self.offset)

        if self.age_standardize:
            age_specific_data = past_data.copy()
            past_data = _get_weights_and_age_standardize(
                past_data, self.gbd_round_id, rescale=self.rescale_age_weights
            )
            # take the difference of age-specific and age-standardized data in last past year
            self.age_ratio = (
                age_specific_data
                - past_data.sel(age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True)
            ).sel(year_id=self.years.past_end)

        return past_data

    def post_process(
        self, modeled_data: xr.DataArray, past_data: xr.DataArray
    ) -> xr.DataArray:
        """Transform modeled_data from logit into linear space, intercept_shift if requested.

        past_data is expected to be the same as what was passed to the corresponding
        pre_process, and should still be in linear space.
        """
        if self.age_standardize:
            modeled_data = (
                modeled_data.sel(
                    age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True
                )
                + self.age_ratio
            )

        if self.remove_zero_slices:
            past_data, _ = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )

        if self.intercept_shift_type == "mean":
            past_data = mean_of_draws(past_data)

        modeled_data = invlogit_with_offset(
            modeled_data, self.offset, bias_adjust=self.bias_adjust
        )

        if (
            self.intercept_shift_type == "mean"
            or self.intercept_shift_type == "unordered_draw"
            or self.intercept_shift_type == "ordered_draw"
        ):
            modeled_data = self.intercept_shift(
                modeled_data=modeled_data,
                past_data=past_data,
                offset=self.offset,
                intercept_shift=self.intercept_shift_type,
                years=self.years,
                shift_from_reference=self.shift_from_reference,
            )

        if self.zero_slices_dict:
            return _add_all_zero_slices(modeled_data, self.zero_slices_dict)

        return modeled_data

    @classmethod
    def intercept_shift(
        cls,
        modeled_data: xr.DataArray,
        past_data: xr.DataArray,
        intercept_shift: str,
        years: YearRange,
        shift_from_reference: bool,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
    ) -> xr.DataArray:
        """Move past and modeled data to logit space, intercept-shift, and translate back.

        Performs the intercept-shift with the type given in ``intercept_shift``, using
        ``years`` and ``shift_from_reference`` as parameters (see docs for
        ``apply_intercept_shift``).

        Translation back from

        Args:
            modeled_data (xr.DataArray): future data to shift. Will be aligned with the
                last-past-year data in ``past_data``.
            past_data (xr.DataArray): past data, the basis to which to shift. Should overlap
                with modeled_data in the last-past-year in ``years``.
            offset (float): Offset for the log transform (see ``logit_with_offset``) and
                inverse transform.
            intercept_shift (str): Type of intercept-shift to perform. See
                ``apply_intercept_shift`` for the options.
            years (YearRange): Years describing the range of years in the past_data and
                modeled_data. Those DataArrays should both contain data for the last-past-year
                (middle component).
            shift_from_reference (bool): When True, the shift is calculated once from the
                future reference scenario and applied to all other scenarios. When False, each
                scenario has its own shift applied to match past data.
        """
        past_data = logit_with_offset(past_data, offset)
        modeled_data = logit_with_offset(modeled_data, offset)
        modeled_data = apply_intercept_shift(
            modeled_data,
            past_data,
            intercept_shift,
            years,
            shift_from_reference,
        )
        modeled_data = invlogit_with_offset(modeled_data, offset, bias_adjust=False)
        return modeled_data


def intercept_shift_in_logit(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    intercept_shift: str,
    years: YearRange,
    shift_from_reference: bool,
    offset: float = ProcessingConstants.DEFAULT_OFFSET,
) -> xr.DataArray:
    """See LogitProcessor.intercept_shift."""
    return LogitProcessor.intercept_shift(
        modeled_data=modeled_data,
        past_data=past_data,
        offset=offset,
        intercept_shift=intercept_shift,
        years=years,
        shift_from_reference=shift_from_reference,
    )


class NoTransformProcessor(BaseProcessor):
    """A Processor that doesn't do any big space transformation.

    But, it can remove zero slices, take the mean, and age-standardize, depending on
    parameters to the constructor.
    """

    def __init__(
        self,
        years: YearRange,
        gbd_round_id: int,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
        remove_zero_slices: bool = False,
        no_mean: bool = False,
        bias_adjust: bool = False,
        intercept_shift: Optional[str] = None,
        age_standardize: bool = False,
        rescale_age_weights: bool = True,
        shift_from_reference: bool = True,
        tolerance: float = ProcessingConstants.DEFAULT_PRECISION,
        **kwargs: Any,
    ) -> None:
        """Construct a LogProcessor with additional processing as specified by the args.

        Args:
            years (YearRange): Forecasting time series year range (used for age-standardize
                and intercept shifting).
            gbd_round_id (int): Which gbd_round_id the data is from (can be used in age
                standardization)
            offset (float): Optional. How much to offset the data from zero when taking the
                log.
            remove_zero_slices (bool): Optional. If True, remove slices of the data that are
                all zeros. Default False
            no_mean (bool): Optional. If True, the mean will not be taken (note that most
                model.py classes expect that the input dependent data will not have draws)
            bias_adjust (bool): Optional. Whether or not to perform bias adjustment on the
                results coming out of log space. Generally ``True`` is the recommended option
            intercept_shift (str): Optional. What type of intercept shift to perform, if any.
                Options are "mean", "unordered_draw", and "ordered_draw".
            age_standardize (bool): Optional. Whether to age-standardize data for modeling
                purposes (results are age-specific). If you want to have age-standardized data
                modeled and the results also be age-standardized, then you should
                age-standardize the data outside of the processor.
            rescale_age_weights (bool): Whether to rescale the age weights across available
                ages when age-standardizing.
            shift_from_reference (bool): Optional. Whether to calculate the differences used
                for the intercept shift with only the reference scenario (True), or calculate
                scenario-specific differences (False). Defaults to True.
            tolerance (float): tolerance value to supply to the "closeness" check. Defaults to
                ``ProcessingConstants.DEFAULT_PRECISION``
            kwargs: Ignored.
        """
        self.years = years
        self.offset = offset
        self.gbd_round_id = gbd_round_id
        self.remove_zero_slices = remove_zero_slices
        self.no_mean = no_mean
        self.bias_adjust = bias_adjust
        self.intercept_shift_type = intercept_shift
        self.age_standardize = age_standardize
        self.rescale_age_weights = rescale_age_weights
        self.zero_slices_dict = {}
        self.age_ratio = xr.DataArray()
        self.shift_from_reference = shift_from_reference
        self.tolerance = tolerance

    def __eq__(self, other: Any) -> bool:
        """True if the other object matches this one in a few aspects."""
        if type(self) != type(other):
            return False
        elif self.offset != other.offset:
            return False
        elif self.years != other.years:
            return False
        elif self.gbd_round_id != other.gbd_round_id:
            return False
        else:
            return True

    def pre_process(self, past_data: xr.DataArray, *args: Any) -> xr.DataArray:
        """Just do the "several other things" that are expected of Processors."""
        if len(args) != 0:
            logger.warning(
                "Warning: passed extra args to NoTransform.pre_process. They will be ignored."
            )

        if self.remove_zero_slices:
            past_data, self.zero_slices_dict = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )

        if not self.no_mean:
            past_data = mean_of_draws(past_data)

        if self.age_standardize:
            age_specific_data = past_data.copy()
            past_data = _get_weights_and_age_standardize(
                past_data, self.gbd_round_id, rescale=self.rescale_age_weights
            )
            self.age_ratio = (
                age_specific_data
                - past_data.sel(age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True)
            ).sel(year_id=self.years.past_end)

        return past_data

    def post_process(
        self, modeled_data: xr.DataArray, past_data: xr.DataArray
    ) -> xr.DataArray:
        """Do no-op transformation, but intercept_shift if requested.

        past_data is expected to be the same as what was passed to the corresponding
        pre_process, and should still be in linear space.
        """
        if self.age_standardize:
            modeled_data = (
                modeled_data.sel(
                    age_group_id=AgeConstants.STANDARDIZED_AGE_GROUP_ID, drop=True
                )
                + self.age_ratio
            )

        if self.remove_zero_slices:
            past_data, _ = _remove_all_zero_slices(
                data=past_data,
                dims=[DimensionConstants.AGE_GROUP_ID, DimensionConstants.SEX_ID],
                tolerance=self.tolerance,
            )
        else:
            past_data = past_data

        if (
            self.intercept_shift_type == "mean"
            or self.intercept_shift_type == "unordered_draw"
            or self.intercept_shift_type == "ordered_draw"
        ):
            modeled_data = self.intercept_shift(
                modeled_data=modeled_data,
                past_data=past_data,
                offset=self.offset,
                intercept_shift=self.intercept_shift_type,
                years=self.years,
                shift_from_reference=self.shift_from_reference,
            )

        if self.zero_slices_dict:
            return _add_all_zero_slices(modeled_data, self.zero_slices_dict)

        return modeled_data

    @classmethod
    def intercept_shift(
        cls,
        modeled_data: xr.DataArray,
        past_data: xr.DataArray,
        intercept_shift: str,
        years: YearRange,
        shift_from_reference: bool,
        offset: float = ProcessingConstants.DEFAULT_OFFSET,
    ) -> xr.DataArray:
        """Intercept-shift future to past, in this "null" space, i.e. just intercept-shift."""
        return apply_intercept_shift(
            modeled_data,
            past_data,
            intercept_shift,
            years,
            shift_from_reference,
        )


def logit_with_offset(
    data: xr.DataArray, offset: float = ProcessingConstants.DEFAULT_OFFSET
) -> xr.DataArray:
    """Apply the logit transformation with an offset adjustment.

    We use an ofset to enforce the range of the logit function while maintaining rotational
    symmetry about (0.5, 0).

    Args:
        data (xr.DataArray): Data to transform.
        offset (float): Amount to offset away from 0 and 1 before logit transform.

    Returns:
        logit_data (xr.DataArray):
            Data transformed into logit space with offset

    Raises:
        RuntimeError: If there are Infs/Nans after transformation
    """
    off = 1 - offset
    norm = 0.5 * offset if offset > 0 else 0
    offset_data = data * off + norm
    logit_data = np.log(offset_data / (1 - offset_data))

    # Verify transformation, ie no infinite values (except missings)
    msg = "There are Infs/Nans after transformation!"
    if not np.isfinite(logit_data).all():
        logger.error(msg)
        raise RuntimeError(msg)

    return logit_data


def bias_correct_invlogit(data: xr.DataArray) -> xr.DataArray:
    """Transform out of logit space and perform an adjustment for bias.

    The adjustment in bias is due to difference in mean of logit draws vs logit of mean draws.

    Pre-conditions:
        * ``draw`` dimension exists in ``data``

    Args:
        data (xr.DataArray):
            Data to perform inverse logit transformation with scaling on

    Returns:
        xr.DataArray:
            The data taken out of logit space and adjusted with scaling methods
            to account for bias (same dims/coords as data)
    """
    expit_data = np.exp(data) / (1 + np.exp(data))
    mean_data = data.mean(DimensionConstants.DRAW)
    inv_mean = np.exp(mean_data) / (1 + np.exp(mean_data))
    adj_expit_data = expit_data - (expit_data.mean(DimensionConstants.DRAW) - inv_mean)

    return adj_expit_data


def invlogit_with_offset(
    data: xr.DataArray,
    offset: float = ProcessingConstants.DEFAULT_OFFSET,
    bias_adjust: bool = True,
) -> xr.DataArray:
    """Reverse logit transform with inherent offset adjustments.

    Recall that we do a logit transform with an adjustment to squeeze 0s and 1s to fit logit
    assumptions while maintaining logit centering at .5.

    Optionally use `bias_correct_invlogit` to obtain the expit data before correcting the
    offset instead of plain logit function.

    Note: If negative values exist after back-transformation, then they are filled with zero.

    Args:
        data (xr.DataArray):
            Data to inverse logit transform (expit).
        offset (float):
            Amount that was offset away from 0 and 1 in the logit transform.
        bias_adjust (bool):
            Whether to apply bias_correct_invlogit instead of "expit".

    Returns:
        expit_data (xr.DataArray):
            Data transformed back into normal space out of logit space with no offset.

    Raises:
        RuntimeError: If there are negative values after back-transformation.
    """
    off = 1 - offset
    norm = 0.5 * offset if offset > 0 else 0

    if bias_adjust:
        expit_data = bias_correct_invlogit(data)
    else:
        expit_data = np.exp(data) / (1 + np.exp(data))
    reset_data = ((expit_data - norm) / off).clip(min=0, max=1)

    msg = "There are negatives after back-transformation!"
    if (reset_data < 0).any():
        logger.error(msg)
        raise RuntimeError(msg)

    return reset_data


def log_with_offset(
    data: xr.DataArray, offset: float = ProcessingConstants.DEFAULT_OFFSET
) -> xr.DataArray:
    """Take the log of data, applying a slight offset to control for the potential of zeros.

    Args:
        data (xr.DataArray):
            Data to log transform
        offset (float):
            Amount to offset away from 0 before the log transform

    Returns:
        log_data (xr.DataArray):
            Data transformed into log space with offset

    Raises:
        RuntimeError: If there are infs/NaNs after transformation.
    """
    log_data = np.log(data + offset)

    # Verify transformation, ie no infinite values (except missings)
    msg = "There are Infs/Nans after transformation!"
    if not np.isfinite(log_data).all():
        logger.error(msg)
        raise RuntimeError(msg)

    return log_data


def log_with_caps(data: xr.DataArray, log_min: float, log_max: float) -> xr.DataArray:
    """Take the log of data, pinning the data to within the [min,max] range.

    Args:
        data (xr.DataArray):
            Data to log transform
        log_min (float):
            Log minimum to clip data to
        log_max (float):
            Log maximum to clip data to

    Returns:
        log_data (xr.DataArray):
            Data transformed into log space with offset

    Raises:
        RuntimeError: If there are infs/NaNs after transformation.
    """
    log_data = np.log(data).clip(min=log_min, max=log_max)

    # Verify transformation, ie no infinite values (except missings)
    msg = "There are Infs/Nans after transformation!"
    if not np.isfinite(log_data).all():
        logger.error(msg)
        raise RuntimeError(msg)

    return log_data


def invlog_with_offset(
    data: xr.DataArray,
    offset: float = ProcessingConstants.DEFAULT_OFFSET,
    bias_adjust: bool = True,
) -> xr.DataArray:
    """Undo a log transform & subtract the offset that was added in the log preprocessing.

    With bias_adjust=True, use the `bias_exp_new` function to adjust the results such that the
    mean of the exponentiated distribution is equal to the exponentiated expected value of the
    log distribution. The adjustment assumes that the log distribution is normally distributed.

    Args:
        data (xr.DataArray):
            Data to inverse log transform (exp)
        offset (float):
            Amount that was offset away from 0 in the log transform
        bias_adjust: If true, apply the ``bias_exp_new`` function instead of ``exp``.

    Returns:
        exp_data (xr.DataArray):
            Data transformed out of log space with no offset

    Raises:
        RuntimeError: If there are negatives after back-transformation
    """
    if bias_adjust:
        exp_data = (bias_exp_new(data) - offset).clip(min=0)
    else:
        exp_data = (np.exp(data) - offset).clip(min=0)

    msg = "There are negatives after back-transformation!"
    if (exp_data < 0).any():
        logger.error(msg)
        raise RuntimeError(msg)

    return exp_data


def _get_weights_and_age_standardize(
    da: xr.DataArray, gbd_round_id: int, rescale: bool = True
) -> xr.DataArray:
    """Age-standardize an xarray.

    We drop weights to ages in the array and renormalize weights to sum to 1.

    Args:
        da: Data to standardize.
        gbd_round_id: the round ID the age weights should come from.
        rescale: Whether to renormalize across the age groups available.

    Returns:
        The input ``da``, standardized.
    """
    age_weights = age.get_age_weights(gbd_round_id)
    age_weights = age_weights.loc[
        age_weights.age_group_id.isin(da.coords["age_group_id"].values), :
    ]

    age_weights = xr.DataArray(
        age_weights.age_weight.values,
        dims=["age_group_id"],
        coords={"age_group_id": age_weights.age_group_id.values},
    )

    age_std_da = age_standardize.age_standardize(da, age_weights, rescale=rescale)
    age_std_da.name = da.name

    return age_std_da


def subset_to_reference(
    data: xr.DataArray,
    draws: Optional[int],
    year_ids: Optional[List[int]] = None,
) -> xr.DataArray:
    """Filter to the given years and draws, and the reference scenario.

    Args:
        data (xr.DataArray):
            The dependent variable data that has not been filtered to relevant
            coordinates
        draws (int | None):
            Either the number of draws to resample to or ``None``, which
            indicates that no draw-resampling should happen.
        year_ids (list[int] | None):
            Optional. The coords of the ``year_id`` dim to filter to. If ``None``, then
            ``year_id`` dim's coords won't be filtered. Defaults to ``None``.

    Returns:
        xr.DataArray:
            cleaned/filtered dependent variable data
    """
    data = get_dataarray_from_dataset(data)

    if "scenario" in data.dims:
        data = data.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD, drop=True)

    if year_ids is not None:
        data_time_slice = data.sel(year_id=year_ids)
    else:
        data_time_slice = data

    if draws:
        if "draw" in data_time_slice.dims:
            data_time_slice = resample(data_time_slice, draws)
        else:
            data_time_slice = expand_dimensions(data_time_slice, draw=np.arange(0, draws))

    return data_time_slice


def clean_cause_data(
    data: xr.DataArray,
    stage: str,
    acause: str,
    draws: Optional[int],
    gbd_round_id: int,
    year_ids: Optional[Iterable[int]] = None,
    national_only: bool = False,
) -> xr.DataArray:
    """Filter the dependent variable data to only relevant most detailed coordinates.

    Also filters out age and sex restrictions.

    Args:
        data (xr.DataArray):
            The dependent variable data that has not been filtered to relevant
            coordinates
        stage (str):
            The GBD/FHS stage that the dependent variable is, e.g. ``pi_ratio``
        acause (str):
            The GBD cause of the dependent variable, e.g. ``cvd_ihd``.
        draws (int | None):
            Either the number of draws to resample to or ``None``, which
            indicates that no draw-resampling should happen.
        gbd_round_id (int):
            Numeric ID for the GBD round
        year_ids (list[int] | None):
            Optional. The coords of the ``year_id`` dim to filter to. If ``None``, then
            ``year_id`` dim's coords won't be filtered. Defaults to ``None``.
        national_only (bool): Whether to include subnational locations, or to include only
            nations.

    Returns:
        xr.DataArray:
            cleaned/filtered dependent variable data
    """
    data = get_dataarray_from_dataset(data)

    if year_ids is not None:
        data_time_slice = data.sel(year_id=year_ids)
    else:
        data_time_slice = data

    if draws is not None:
        data_time_slice = resample(data_time_slice, draws)

    cleaned_data, warning_msg = _filter_relevant_coords(
        data_time_slice, acause, stage, gbd_round_id, national_only
    )
    return cleaned_data, warning_msg


def clean_covariate_data(
    past: xr.DataArray,
    forecast: xr.DataArray,
    dep_var: xr.DataArray,
    years: YearRange,
    draws: int,
    gbd_round_id: int,
    national_only: bool = False,
) -> xr.DataArray:
    """Combines past and forecasted covariate data into one array.

    Filters dims to only relevant most-detailed coords.

    Raises IndexError if the past and forecasted data dims don't line up (after the past is
    broadcast across the scenario dim).

    Args:
        past (xr.DataArray): Past covariate data
        forecast (xr.DataArray):
            Forecasted covariate data
        dep_var (xr.DataArray):
            Dependent variable data that has already been filtered down to relevant
            coordinates. Relevant coordinates will be inferred from this array.
        years (YearRange):
            Forecasting timeseries
        draws (int):
            Number of draws to include.
        gbd_round_id (int):
            Numeric ID for the GBD round.
        national_only (bool): Whether to include subnational locations, or to include only
            nations.

    Returns:
        xr.DataArray: Cleaned covariate data.

    Raises:
        IndexError: If
            * the past and forecasted data coords don't line up.
            * the covariate data is missing coordinates from a dim it shares with the
              dependent variable.
    """
    cov_name = forecast.name
    only_forecast = forecast.sel(year_id=years.forecast_years)

    resampled_forecast = ensure_draws(only_forecast, draws)
    stripped_forecast = strip_single_coord_dims(resampled_forecast)

    only_past = past.sel(year_id=years.past_years)

    resampled_past = ensure_draws(only_past, draws)
    stripped_past = strip_single_coord_dims(resampled_past)

    # expand dims if scenario dim in future but not past
    data = concat_past_and_future(stripped_past, stripped_forecast)

    if gbd_round_id in GBDRoundIdConstants.NO_MOST_DETAILED_IDS:
        most_detailed_data = data
    else:
        most_detailed_data = filter.make_most_detailed(data, gbd_round_id, national_only)

    shared_dims = list(
        set(most_detailed_data.dims) & set(dep_var.dims) - {DimensionConstants.YEAR_ID}
    )
    expected_coords = {dim: list(dep_var[dim].values) for dim in shared_dims}
    # Don't modify draws: We resampled the `most_detailed_data` but not the `dep_var`.
    if DimensionConstants.DRAW in expected_coords:
        del expected_coords[DimensionConstants.DRAW]
    try:
        cleaned_data = most_detailed_data.sel(**expected_coords)
    except KeyError:
        err_msg = f"`{cov_name}` is missing expected coords"
        logger.error(err_msg)
        raise IndexError(err_msg)

    if not np.isfinite(cleaned_data).all():
        err_msg = f"`{cov_name}` past and forecast coords don't line up`"
        logger.error(err_msg)
        raise IndexError(err_msg)

    return cleaned_data.rename(cov_name)


def ensure_draws(da: xr.DataArray, draws: Optional[int]) -> xr.DataArray:
    """Resample ``da``, ensuring that it winds up with a ``draw`` dimension.

    This acts just like ``resample`` except that it adds the ``draw`` dimension if missing.

    Args:
        da: the data to resample, if it has a ``draws`` dimension.
        draws: The desired number of draws.

    Returns:
        The resampled data
    """
    if DimensionConstants.DRAW in da.dims:
        return resample(da, draws)
    elif draws:
        return expand_dimensions(da, draw=list(range(draws)))
    else:
        return da


def mad_truncate(
    da: xr.DataArray,
    median_dims: Iterable[str],
    pct_coverage: float,
    max_multiplier: float,
    multiplier_step: float,
) -> xr.DataArray:
    """Truncate values based on the median absolute deviation.

    Calculates the median absolute deviation (MAD) across coordinates for each median_dims,
    then finds floor and ceiling values based on a multiplier for the MAD that covers
    pct_coverage of the data (only one multiplier value across whole dataarray. This could
    change to separate multipliers for each of the median_dims coordinates, but is based on the
    method from the legacy code right now). Data is truncated to be between the floors and
    ceilings.

    This is used as a more flexible floor/ceiling truncation method than hard cutoffs. It is
    used in the `indicator_from_ratio.py` script to control for extreme values, as dividing to
    obtain the indicator from the ratio can sometimes lead to very high values if the
    denominator is low and/or the numerator is high. The primary concern is the MI and MP
    ratios, which have demonstrated extreme value problems stemming from the division of M by
    MI or MP. However, it is a flexible method for truncating that could be used in other
    situations outside of `indicator_from_ratio.py` as well.

    Note:
        The MAD calculated by _mad is multiplied by a default scale of 1.4826
        for consistency with `scipy.stats.median_absolute_deviation`.

    Args:
        da (xr.DataArray):
            The array to calculate the multiplier for.
        median_dims (list[str]):
            List of dims to calculate the MAD for. E.g. if median_dims is ['age_group_id'],
            then median will return a dataarray with only age_group_id dimension).
        pct_coverage (float):
            Percent of data in array to have between the floor and ceiling, e.g. 0.975.
        max_multiplier (float):
            The maximum multiplier for the MAD that is acceptable. If it is too small, then
            pct_coverage of the data might not be between median +/- multiplier * MAD.
        multiplier_step (float):
            The amount to test multipliers by (starting value is zero + step).

    Returns:
        xr.DataArray:
            da truncated to be between ceiling and floor based on the MAD
    """

    def _mad_truncate(
        da: xr.DataArray,
        median_dims: Iterable[str],
        pct_coverage: float,
        max_multiplier: float,
        multiplier_step: float,
    ) -> xr.DataArray:
        """Helper function for `mad_truncate`."""
        dims_to_median = set(da.dims).difference(set(median_dims))
        mad_da = _mad(da, median_dims)
        median_da = da.median(dim=dims_to_median)
        multiplier = _calculate_mad_multiplier(
            da,
            mad_da,
            median_da,
            pct_coverage,
            max_multiplier=max_multiplier,
            step=multiplier_step,
        )
        ceiling = median_da + (multiplier * mad_da)
        floor = median_da - (multiplier * mad_da)
        truncated_da = da.where(da < ceiling, other=ceiling)
        truncated_da = truncated_da.where(da > floor, other=floor)
        return truncated_da

    if "scenario" in da.dims:
        truncated_scenarios_list = []
        for scenario in da[DimensionConstants.SCENARIO].values:
            sub_da = da.sel(scenario=scenario)
            truncated_sub_da = _mad_truncate(
                sub_da, median_dims, pct_coverage, max_multiplier, multiplier_step
            )
            truncated_scenarios_list.append(truncated_sub_da)
        all_scenarios_truncated = xr.concat(
            truncated_scenarios_list, dim=DimensionConstants.SCENARIO
        )
    else:
        all_scenarios_truncated = _mad_truncate(
            da, median_dims, pct_coverage, max_multiplier, multiplier_step
        )

    all_scenarios_truncated = all_scenarios_truncated.transpose(*da.dims)

    return all_scenarios_truncated


def strip_single_coord_dims(da: xr.DataArray) -> xr.DataArray:
    """Strip off single coordinate dimensions and point coordinates.

    Args:
        da (xr.DataArray):
            The array to strip.

    Returns:
        xr.DataArray:
            Array without single coord dims or point coords, but without any other changes.
    """
    stripped_da = da.copy()
    for dim in list(da.coords):
        if dim not in da.dims:
            # dim is a point coord
            stripped_da = stripped_da.drop_vars(dim)
        elif len(da[dim].values) == 1:
            # dim has only one coord
            stripped_da = stripped_da.sel({dim: da[dim].values[0]}, drop=True)
    return stripped_da


def expand_single_coord_dims(new_da: xr.DataArray, ref_da: xr.DataArray) -> xr.DataArray:
    """Expand dataarray to include point coords and single coord dims per the "ref" dataarray.

    Args:
        new_da (xr.DataArray):
            The dataarray to expand
        ref_da (xr.DataArray):
            The dataarray to infer point coords and/or single coord dims from

    Returns:
        xr.DataArray:
            The expanded copy of ``new_da`` dataarray
    """
    expanded_da = new_da.copy()
    for dim in list(ref_da.coords):
        if dim not in ref_da.dims:
            # dim is a point coord
            coord = ref_da[dim].values
            expanded_da = expanded_da.assign_coords(**{dim: coord})
        elif len(ref_da[dim].values) == 1:
            # dim has only one coord
            coord = ref_da[dim].values[0]
            expanded_da = expanded_da.expand_dims(**{dim: [coord]})
    return expanded_da


def get_dataarray_from_dataset(ds: Union[xr.DataArray, xr.Dataset]) -> xr.DataArray:
    """Extract a DataArray from a Dataset, or just return the DataArray if given one."""
    if isinstance(ds, xr.Dataset):
        try:
            return ds["value"]
        except KeyError:
            return ds["sdi"]
    else:
        return ds


def remove_unexpected_dims(da: xr.DataArray) -> xr.DataArray:
    """Remove unexpected single-coord dimensions or point-coordinates.

    Also, asserts that optional or unexpected dims either have just one coord or are
    point-coords.

    Args:
        da (xr.DataArray):
            The dataarray to make conform to the expected dims

    Returns:
        xr.DataArray:
            The original dataarray, but with unexpected dims removed

    Raises:
        IndexError: If optional or unexpected dim has more than one coord
    """
    for dim in da.coords:
        if dim in ProcessingConstants.EXPECTED_DIMENSIONS["optional"]:
            try:
                num_coords = len(da[dim])
                if num_coords > 1:
                    err_msg = f"{dim} is an optional dim with more than one coord"
                    logger.error(err_msg)
                    raise IndexError(err_msg)
            except TypeError:
                pass  # dim is a point coord, and that's okay in this case
        elif dim not in ProcessingConstants.EXPECTED_DIMENSIONS["required"]:
            da = _remove_unexpected_dim(da, dim)

    return da


def _remove_unexpected_dim(da: xr.DataArray, dim: str) -> xr.DataArray:
    try:
        num_coords = len(da[dim])
        if num_coords > 1:
            err_msg = f"{dim} is an unexpected dim with more than one coord"
            logger.error(err_msg)
            raise IndexError(err_msg)
        else:
            # dim is a single coord-dimension
            da = da.sel({dim: da[dim].values[0]}, drop=True)
    except TypeError:
        # dim is a point-coord
        da = da.drop_vars(dim)

    return da


def concat_past_and_future(past: xr.DataArray, future: xr.DataArray) -> xr.DataArray:
    """Concatenate past and future data by expanding past scenario dimension.

    Does not account for mismatched coordinates at the moment.

    Prerequisites:
      * No past scenario dimension
      * No overlapping years
      * Matching dims except for year and scenario

    Args:
        past (xr.DataArray):
            The past dataarray to concatenate
        future (xr.DataArray):
            The forecast dataarray to concatenate

    Returns:
        xr.DataArray:
            The complete time series data with past and future.

    Raises:
        IndexError: If dimensions other than year and scenario do not line up.
    """
    if (
        DimensionConstants.SCENARIO in future.dims
        and DimensionConstants.SCENARIO not in past.dims
    ):
        past = past.expand_dims(scenario=future[DimensionConstants.SCENARIO].values)

    inconsistent_dims = set(past.dims).symmetric_difference(set(future.dims))
    if inconsistent_dims:
        err_msg = "Dimensions don't line up for past and future"
        logger.error(err_msg)
        raise IndexError(err_msg)

    data = xr.concat([past, future], dim=DimensionConstants.YEAR_ID)

    return data


def _filter_relevant_coords(
    data: xr.DataArray, acause: str, stage: str, gbd_round_id: int, national_only: bool = False
) -> xr.DataArray:
    """Filter dataarray to the relevant most-detailed coords for the given cause and stage.

    Args:
        data: Data to filter.
        acause: acause to determine the restrictions to apply.
        stage: stage to determine the restrictions to apply.
        gbd_round_id: gbd_round_id to determine the restrictions to apply.
        national_only: whether to also filter down to national locations only.

    Returns:
        The filtered version of ``data``.
    """
    inferred_stage = restrictions.get_stage_to_infer_restrictions(stage)
    if stage != inferred_stage:
        logger.debug(f"Inferring demographic restrictions for {stage}, from {inferred_stage}")

    stage_restrictions = restrictions.get_restrictions(acause, inferred_stage, gbd_round_id)
    sliced_data = data.sel(**stage_restrictions)
    most_detailed_data = filter.make_most_detailed(
        sliced_data, gbd_round_id, national_only=national_only
    )

    filled_with_age_data, warning_msg = _fill_age_restrictions(
        most_detailed_data, acause, stage, gbd_round_id
    )

    return filled_with_age_data, warning_msg


def _fill_age_restrictions(
    data: xr.DataArray, acause: str, stage: str, gbd_round_id: int
) -> xr.DataArray:
    """Expand ``data`` to include missing needed age-groups.

    The "needed" age-groups are dependent on the acause, stage, and gbd_round_id.

    The missing groups are filled with the nearest available age-groups.

    Args:
        data: DataArray to expand.
        acause: The "acause" whose restrictions should apply.
        stage: The "stage" of processing whose restrictions should apply.
        gbd_round_id: The gbd round ID from which to load age-group data.

    Returns:
        The filled-out version of ``data``.

    Raises:
        RuntimeError: if we have a missing age group that is a "middle" age group -- one
            "between" available age-groups.
    """
    available_age_ids = list(data["age_group_id"].values)
    # If we are given mortality, for example, we want to get the age
    # availability of the measure/cause we are calculating ratio with.
    inferred_stage = restrictions.get_stage_to_infer_restrictions(stage, purpose="non_fatal")

    needed_age_ids = restrictions.get_restrictions(acause, inferred_stage, gbd_round_id)[
        "age_group_id"
    ]

    missing_age_ids = list(set(needed_age_ids) - set(available_age_ids))
    if missing_age_ids:
        logger.warning(f"age-group-ids:{missing_age_ids} are missing")
        # Get oldest available age group and its data
        oldest_avail_age_id = _get_oldest_age_id(available_age_ids, gbd_round_id)
        oldest_avail_data = data.sel(age_group_id=oldest_avail_age_id, drop=True)

        # Get youngest available age group and its data
        youngest_avail_age_id = _get_youngest_age_id(available_age_ids, gbd_round_id)
        youngest_avail_data = data.sel(age_group_id=youngest_avail_age_id, drop=True)

        # Sort missing age groups into old and young
        missing_old_age_ids = []
        missing_young_age_ids = []
        for age_id in missing_age_ids:
            if _AgeGroupID(age_id, gbd_round_id) > oldest_avail_age_id:
                missing_old_age_ids.append(age_id)
            elif _AgeGroupID(age_id, gbd_round_id) < youngest_avail_age_id:
                missing_young_age_ids.append(age_id)
            else:
                err_msg = (
                    "age_group_id={age_id} is a middle age group -- it is"
                    "between available age-groups. This is unexpected."
                )
                logger.error(err_msg)
                raise RuntimeError(err_msg)

        warning_template = (
            "age_group_id={} are being filled with the " "data from age_group_id={}. "
        )
        # Expand to include old age groups
        with_missing_old_data = expand_dimensions(
            data, age_group_id=missing_old_age_ids, fill_value=oldest_avail_data
        )
        # Expand to include young age groups
        with_missing_young_data = expand_dimensions(
            with_missing_old_data,
            age_group_id=missing_young_age_ids,
            fill_value=youngest_avail_data,
        )

        warning_msg = ""
        if missing_old_age_ids:
            warning_msg += warning_template.format(
                missing_old_age_ids, int(oldest_avail_age_id)
            )
        if missing_young_age_ids:
            warning_msg += warning_template.format(
                missing_young_age_ids, int(youngest_avail_age_id)
            )

        if warning_msg:
            logger.warning(warning_msg)

        return with_missing_young_data, warning_msg
    else:
        return data.copy(), None


def make_shared_dims_conform(
    to_update: xr.DataArray, reference: xr.DataArray, ignore_dims: Optional[List[str]] = None
) -> xr.DataArray:
    """Make a given array conform to another on certain shared dimensions.

    Note:
        * ``to_update`` is expected to have all the same coords and maybe extra
          for all the shared dims excluding the dims in ``ignore_dims``.

    Args:
        to_update (xr.DataArray):
            To make conform
        reference (xr.DataArray):
            Dataarray to conform to
        ignore_dims (list[str]):
            list of dims to omit from conforming e.g. DimensionConstants.YEAR_ID

    Returns:
        ``to_update`` that has been filtered to conform to ``reference``
    """
    ignore_dims = ignore_dims or []
    coord_dict = {}
    for dim in reference.dims:
        if dim not in ignore_dims and dim in to_update.dims:
            coord_dict.update({dim: reference[dim].values})

    return to_update.sel(coord_dict)


class _AgeGroupID(object):
    """Abstraction for age-group IDs.

    Basically just supports comparisons, to see which of two represents a younger/older
    age-group.
    """

    def __init__(self, age_id: int, gbd_round_id: int) -> None:
        self.age_id = age_id
        self.gbd_round_id = gbd_round_id

    def __gt__(self, other: int) -> bool:
        return self.age_id == _get_oldest_age_id([self.age_id, other], self.gbd_round_id)

    def __lt__(self, other: int) -> bool:
        return self.age_id == _get_youngest_age_id([self.age_id, other], self.gbd_round_id)


def _get_oldest_age_id(age_group_ids: List[int], gbd_round_id: int) -> int:
    """Among the given age-groups, return the oldest one."""
    age_df = age.get_ages(gbd_round_id)[["age_group_id", "age_group_years_start"]]
    relevant_age_df = age_df.query("age_group_id in @age_group_ids")
    oldest_age_id = relevant_age_df.loc[relevant_age_df["age_group_years_start"].idxmax()][
        "age_group_id"
    ]
    return oldest_age_id


def _get_youngest_age_id(age_group_ids: List[int], gbd_round_id: int) -> int:
    """Among the given age-groups, return the youngest one."""
    age_df = age.get_ages(gbd_round_id)[["age_group_id", "age_group_years_start"]]
    relevant_age_df = age_df.query("age_group_id in @age_group_ids")
    youngest_age_id = relevant_age_df.loc[relevant_age_df["age_group_years_start"].idxmin()][
        "age_group_id"
    ]
    return youngest_age_id


def _remove_all_zero_slices(
    data: xr.DataArray,
    dims: Iterable[str],
    tolerance: float = ProcessingConstants.DEFAULT_PRECISION,
) -> Tuple[xr.DataArray, xr.DataArray]:
    """A method that removes and stores all-zero slices from a dataarray.

    Args:
        data (xr.DataArray): data array that contains dims in its dimensions.
        dims (Iterable): an iterable of dims over which to seek zero-slices.
        tolerance (float): tolerance value to supply to the "closeness" check. Defaults to
            ``ProcessingConstants.DEFAULT_PRECISION``

    Returns:
        (tuple): a tuple of data array and dict.  The data array is the
        input data sans zero-slices.  The dict keeps track of the slices
        that were removed.
    """
    zero_slices_dict = {}  # to help keep track of all-zero slices
    keep_slices_dict = {}  # the complement of zero_slices_dict

    avail_dims = [dim for dim in dims if dim in data.dims]
    for dim in avail_dims:
        zero_coords = []
        keep_coords = []
        for coord in data[dim].values:
            slice = data.sel({dim: coord})
            if np.isclose(a=slice, b=0, atol=tolerance).all():
                zero_coords.append(coord)
            else:
                keep_coords.append(coord)
        if zero_coords:
            zero_slices_dict[dim] = zero_coords
            keep_slices_dict[dim] = keep_coords

    if zero_slices_dict:
        data = data.sel(**keep_slices_dict)

    return data, zero_slices_dict


def _add_all_zero_slices(
    data: xr.DataArray, new_addition_dict: Dict[str, Any]
) -> xr.DataArray:
    """Adds slices of zeros to data array.

    Args:
        data (xr.DataArray): data to be added to.
        new_addition_dict (dict): new zero-slices to have in data.

    Returns:
        (xr.DataArray): data with additional slices that are all zeros.
    """
    return expand_dimensions(data, fill_value=0, **new_addition_dict)


def _mad(da: xr.DataArray, median_dims: Iterable[str], scale: float = 1.4826) -> xr.DataArray:
    """Calculate the median absolute deviation.

    Multiplies by `scale` for consistency with scipy.stats.median_absolute_deviation.

    Args:
        da (xr.DataArray):
            The array to calculate MAD.
        median_dims (list[str]):
            List of dims to calculate the MAD for. Ex.: if median_dims is ['age_group_id'],
            then it will return a dataarray with only age_group_id dimension
        scale (float):
            (Optional.) The scaling factor applied to the MAD. The default scale (1.4826)
            ensures consistency with the standard deviation for normally distributed data.

    Returns:
        xr.DataArray:
            Array with only median_dims containing the MAD for those dims.
    """
    dims_to_median = set(da.dims).difference(set(median_dims))
    return scale * (np.abs(da - da.median(dim=dims_to_median))).median(dim=dims_to_median)


def _calculate_mad_multiplier(
    da: xr.DataArray,
    mad_da: xr.DataArray,
    median_da: xr.DataArray,
    pct_coverage: float,
    max_multiplier: float,
    step: float,
) -> Optional[float]:
    """Multiplier necessary to achieve desired % of data within (median +- multiplier * MAD).

    Args:
        da (xr.DataArray):
            The array to calculate the multiplier for.
        mad_da (xr.DataArray):
            The array that has the median absolute deviation across some dims of da.
        median_da (xr.DataArray):
            The array that has the median across some dims of da.
        pct_coverage (float):
            Percent of data in array to have between the floor and ceiling, e.g. .975.
        max_multiplier (float):
            The maximum multiplier for the MAD that is acceptable. If it is too small, then
            pct_coverage of the data might not be between median +/- multiplier * MAD.
        step (float):
            The amount to test multipliers by (starting value is zero + step).

    Returns:
        float:
            Multiplier for MAD to cap values outside of multiplier * MAD, or None if we don't
            find such a multiplier less than max_multiplier.
    """
    for multiplier in np.arange(0 + step, max_multiplier + step, step):
        ceiling = median_da + (multiplier * mad_da)
        floor = median_da - (multiplier * mad_da)
        between_da = da.where(da > floor).where(da < ceiling)
        vals_between = between_da.count().values.item(0)
        vals_total = da.count().values.item(0)
        if multiplier >= max_multiplier:
            logger.warning(
                (
                    f"Using max_multiplier! {vals_between / vals_total} "
                    f"pct coverage achieved using max multiplier."
                )
            )
            return max_multiplier
        elif (vals_between / vals_total) > pct_coverage:
            return multiplier

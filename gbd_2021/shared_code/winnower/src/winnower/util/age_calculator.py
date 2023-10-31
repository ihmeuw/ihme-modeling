import datetime as dt
import math
import statistics

from attr import attrib
import numpy
import pandas
import re

from winnower import errors
from winnower.extract import get_column
from winnower.logging import get_class_logger
from winnower.transform.base import attrs_with_logger
from winnower.util.dataframe import (
    as_column_type,
    set_default_values,
)
from winnower.util.dtype import (
    is_float_value,
    is_numeric_dtype,
    is_datetime_dtype,
)
from winnower.util.dates import (
    datetime_to_numeric,
    decode_century_month_code,
)
from winnower.util.expressions import (
    build_expression_query,
    is_expression,
)

from winnower.globals import eg  # extraction globals

MAX_RATIO_NEGATIVE_AGE = 0.02  # >= leads to an error
MAX_PCT_VALUES_OUT_OF_RANGE = 0.10  # > leads to an error


class AgeCalculator:
    """
    Attempts to populate "age_year" and "age_months" columns from inputs.
    """
    def __init__(self, columns, config, keys=None):
        if keys is None:
            keys = AgeKeys(prefix='', date_type='int')  # date_type is Interview (int) by default

        self.columns = columns
        self.config = config
        self.k = keys
        self.logger = get_class_logger(self)

    def calculated_columns(self) -> list:
        "Returns str columns this can calculate given the configuration."
        # Always provided - imputed if necessary from survey start/end year
        res = [self.k.year]

        if self._interview_month_configured():
            res.append(self.k.month)

        if self._interview_day_configured():
            res.append(self.k.day)

        if self._can_compute_age_day():
            res.append(self.k.age_day)

        if self._can_compute_age_month():
            res.append(self.k.age_month)

        if self._can_compute_age_year():
            res.append(self.k.age_year)

        return res

    def calculate_ages(self, df):
        scratch = self._scratch_df_factory(df)
        df, scratch = self._process_age_categorical(df, scratch)
        # interview and birth date processing
        scratch = self._process_date_type(df, scratch)
        scratch = self._process_birth_date(df, scratch)
        # set {age,birth,int}_vars
        scratch = self._clean_age_birth_and_date_type_columns(df, scratch)
        # adjust variables
        scratch = self._adjust_year_month_columns(scratch)
        # impute only int_year if missing
        if self.k.year.startswith('int_'):
            scratch = self._impute_missing_int_year(scratch)
        # set logical limits on data
        scratch = self._clip_data_within_limits(scratch)
        # populate age values
        # age can be computed only if date_type is interview (int)
        if self.k.date.startswith('int_'):
            scratch = self._calculate_age_columns(scratch)
            scratch = self._validate_results(scratch)

        df = self._merge_output_columns_into_df(df, scratch)
        return df

    # copy/pasted from winnower.custom.base.TransformBase
    def get_column_name(self, name):
        """
        Returns the actual column name given the case-insensitive match `name`.
        """
        column, _, err = get_column(name, self.columns)
        if column is None:
            raise errors.ValidationError(err.args[0])
        return column

    def _configured(self, *inputs):
        "Predicate: are all input values configured?"
        return all(map(self.config.get, inputs))

    def _scratch_df_factory(self, df):
        """
        Returns a DataFrame to be used as scratch space.

        Some calculations require intermediate fields which don't belong in the
        final output. Rather than repeatedly mutate the DataFrame we're
        returning instead perform all calculations on this DataFrame and then
        merge the necessary columns at the end.
        """
        return pandas.DataFrame(
            {col: df[col] if col in df else float('nan')
             for col in self.k.columns()},
            index=df.index)

    def _process_age_categorical(self, df, scratch):
        """
        Process age_categorical values and convert to numbers.

        Example input may be "24-29 Months" with output 26.5 (the mean).
        """
        # Note: this function directly replicates the original stata logic
        if not self._configured('age_categorical_type',
                                'age_categorical_parse'):
            # TODO: log that it was copied (backwards compatibility)
            return df, scratch

        # split on delimiter. Will have a 0 and 1 column (int, not str)
        age_cat_col = self.get_column_name(self.config['age_categorical'])
        split_df = df[age_cat_col].str.split(
            self.config['age_categorical_parse'],  # delimiter
            n=1,  # 1 split max
            expand=True,  # return DataFrame (defaults to series of lists)
        )

        def extract_as_number(series):
            nums = series.str.extract(r'(\d+)', expand=False)
            try:
                return nums.astype(int)
            except ValueError:
                return nums.astype(float)

        split_df[0] = extract_as_number(split_df[0])
        split_df[1] = extract_as_number(split_df[1])

        k_attr = 'age_' + self.config['age_categorical_type']
        column = getattr(self.k, k_attr)
        scratch[column] = split_df.mean(axis='columns')

        return df, scratch

    def _process_date_type(self, df, scratch):
        config_col_name = self.config.get(self.k.date)
        if config_col_name:  # mutates scratch
            date_column = self.get_column_name(config_col_name)
            col = df[date_column]
            fmt = self.config[self.k.date_format]
            col = self._convert_stata_date(col, fmt, f"{self.k.date}_column")

            self._process_date(scratch, "", col, fmt)
        return scratch

    def _process_birth_date(self, df, scratch):
        config_col_name = self.config.get(self.k.birth_date)
        if config_col_name:  # mutates scratch
            birth_date_column = self.get_column_name(config_col_name)
            col = df[birth_date_column]
            fmt = self.config[self.k.birth_date_format]
            col = self._convert_stata_date(col, fmt, birth_date_column)

            self._process_date(scratch, 'birth_', col, fmt)
        return scratch

    def _convert_stata_date(self, col, fmt, col_name):
        if eg.strict:
            if is_datetime_dtype(col) and not is_SIF(fmt):
                self.logger.warning(f"STRICT: Column {col_name} is already "
                                    " a date. Attempting to convert to "
                                    "days since Jan, 1960!!. Check your data "
                                    "as this may be wrong!")
                # column is already a date ...
                # TODO: An assumption is made here that the birth_date column
                # was originally a stata long column formatted as a date. This
                # is not necessarily true.
                # The following code works if the above assumption holds true
                # but it needs to be re-worked to support other interpretations
                # of date columns.
                col = datetime_to_numeric(col)

        return col

    def _process_date(self, scratch, prefix, date_col, date_fmt):
        extractor = DateExtractor.from_format(date_fmt)
        for col in extractor.yield_date_parts(date_col):
            col.index = scratch.index  # update index before assignment to df
            col.name = getattr(self.k, f"{prefix}{col.name}")
            scratch[col.name] = col

    def _clean_age_birth_and_date_type_columns(self, df, scratch):
        """
        Cleans birth column and other defined by date_type (e.g. interview) in three ways:

        * Ensure values are numeric, convert if necessary.
        * Replace NaNs with those in configured {int,birth,reg}_{day,month,year}
            columns. Only age categorical values can be present at this point.
        * Set values as NaN according to related "_missing" config. This config
            may be a logical expression or a list of explicit values.
        """
        for col_name in self.k.columns():
            self._ensure_column_numeric(scratch, col_name)
            self._replace_col_with_df_column(scratch, df, col_name)
            self._set_missing_values_to_nan(scratch, col_name)

        return scratch

    def _ensure_column_numeric(self, scratch, col_name):
        scratch[col_name] = coerce_column_to_numeric(scratch[col_name])

    def _replace_col_with_df_column(self, scratch, df, col_name):
        "Replace column in scratch with column in df, if it exists."
        # Note: because these indicators have code_custom == True they are
        #       NOT renamed as is usual. Instead we manually retrieve the
        #       column name from self.config instead of knowing that they
        #       are e.g. named "age_year"
        config_col_name = self.config.get(col_name)
        if config_col_name:
            source_col_name = self.get_column_name(config_col_name)
            if not eg.strict:
                # Don't try to reproduce UbCov's behavior.
                scratch[col_name] = df[source_col_name]
            else:
                # Replace only those that are not null if the input column
                # already had data. Otherwise, replace all.
                replace_all = True
                config_name = False

                # Don't process for column that starts with 'age_'. There is no 'age_date' and 'age_date_format'
                if not col_name.startswith('age_'):
                    if col_name.startswith('birth_'):
                        config_name = self.k.birth_date
                        fmt = self.k.birth_date_format
                    else:
                        # For any date_type defined with AgeKeys
                        # by default interview date, column name starts with 'int_')
                        config_name = self.k.date
                        fmt = self.k.date_format
                if config_name:
                    date_source_col_name = self.config.get(config_name)
                    if date_source_col_name and fmt:
                        replace_all = False

                # As we can't check to see if the column was generated an
                # assumption is made here that any column having all nulls was
                # likely not generated and should be replaced. However, this
                # might not work in all cases.
                not_null_mask = scratch[col_name].notna()
                if not replace_all and not_null_mask.sum() > 0:
                    msg = ("STRICT: replace only not null source "
                           f" {source_col_name} column {col_name}.")
                    self.logger.warning(msg)
                    scratch.loc[not_null_mask, col_name] =\
                        df.loc[not_null_mask, source_col_name]
                else:
                    scratch[col_name] = df[source_col_name]

            self._ensure_column_numeric(scratch, col_name)

    def _set_missing_values_to_nan(self, scratch, col_name):
        missing = self.config.get(f"{col_name}_missing")
        if not missing:
            return

        col = scratch[col_name]
        if is_expression(missing):
            missing_expr = build_expression_query(missing, col_name)
            # convert column to DataFrame to use .query
            col_frame = col.to_frame()
            try:
                missing_values = col_frame.query(expr=missing_expr)
            except Exception as e:
                msg = (f"Error cleaning {col_name} with "
                       f"{col_name}_missing expression {missing} - "
                       f"error is {e!r}")
                raise RuntimeError(msg)
            else:
                scratch.loc[missing_values.index, col_name] = numpy.nan
        else:  # list of missing values
            try:
                missing_values = as_column_type(col, values=missing)
            except ValueError:
                msg = (f"One or more values in {missing} could not be "
                       f"converted to type {col.dtype}")
                self.logger.error(msg)
            else:
                missing_indices = col.index[col.isin(missing_values)]
                scratch.loc[missing_indices, col_name] = numpy.nan

    def _adjust_year_month_columns(self, scratch):
        year_adjust = self.config.get('year_adjust', 0)
        if year_adjust:
            scratch.loc[:, self.k.birth_year] += year_adjust
            scratch.loc[:, self.k.year] += year_adjust
            msg = (f"Adjusted {self.k.birth_year} and {self.k.year} by "
                   f"{year_adjust}")
            self.logger.info(msg)

        month_adjust = self.config.get('month_adjust', 0)
        if month_adjust:
            scratch.loc[:, self.k.birth_month] += month_adjust
            scratch.loc[:, self.k.month] += month_adjust

            self._fix_wrapped_months(scratch, 'birth_')
            self._fix_wrapped_months(scratch, '')  # Still need to call this without prefix for 'int_*' column
            msg = (f"Adjusted {self.k.birth_month} and {self.k.month} by "
                   f"{month_adjust}")
            self.logger.info(msg)

        self._convert_two_digit_year(scratch, self.k.birth_year)
        self._convert_two_digit_year(scratch, self.k.year)

        return scratch

    def _fix_wrapped_months(self, scratch, prefix):
        "Wrap years/months due to month_adjust."
        year = getattr(self.k, f"{prefix}year")
        month = getattr(self.k, f"{prefix}month")

        col = scratch[month]

        over = col > 12
        scratch.loc[over, month] -= 12
        scratch.loc[over, year] += 1

        under = col < 1
        scratch.loc[under, month] += 12
        scratch.loc[under, year] -= 1

    def _convert_two_digit_year(self, scratch, column):
        series = scratch[column]
        low_mask = series < 100

        all_values_under_100 = (low_mask | pandas.isnull(series)).all()
        if not all_values_under_100:
            return

        if self.config['year_end'] < 2000:
            low_index = series[low_mask].index
            scratch.loc[low_index, column] += 1900
        else:
            # end of survey (year_end) is 2005
            # threshold is 5
            threshold = self.config['year_end'] % 100
            lte_threshold = series <= threshold
            # birth year 3? assumed to be born 2003, NOT 1903
            # anyone with e.g., birth year <=5 is assumed to be born <= 2005
            scratch.loc[lte_threshold, column] += 2000
            # birth year 7? assumed to be born 1907
            # (2007 is AFTER the survey was administered)
            mask = (~lte_threshold & low_mask)
            scratch.loc[mask, column] += 1900

    def _impute_missing_int_year(self, scratch):
        """
        Imputes int_year when it is missing or outside of valid range.
        """
        self._clip_values_to_range(scratch, self.k.year, Ranges.YEAR_RANGE)
        imputed = math.ceil(statistics.mean([
            self.config['year_start'],
            self.config['year_end'],
        ]))
        set_default_values(self.k.year, imputed, scratch)

        return scratch

    def _clip_values_to_range(self, df, column, valid_range):
        series = df[column]
        low, high = valid_range
        mask = (series < low) | (high < series)

        ratio = mask.sum() / len(mask)
        if ratio > MAX_PCT_VALUES_OUT_OF_RANGE:
            msg = (f"More than {MAX_PCT_VALUES_OUT_OF_RANGE:.2f} out of valid"
                   f"range {valid_range} for field {series.name}")
            self.logger.warning(msg)

        df.loc[mask, column] = numpy.nan

    def _clip_data_within_limits(self, scratch):
        def clip_values(col, range):
            "Clip scratch column according to provided range."
            self._clip_values_to_range(scratch, col, range)

        clip_values(self.k.age_year, Ranges.AGE_YEAR_RANGE)
        clip_values(self.k.age_month, Ranges.AGE_MONTH_RANGE)
        clip_values(self.k.age_day, Ranges.AGE_DAY_RANGE)

        clip_values(self.k.birth_year, Ranges.YEAR_RANGE)
        clip_values(self.k.birth_month, Ranges.MONTH_RANGE)
        clip_values(self.k.birth_day, Ranges.DAY_RANGE)

        # Note: int_year is clipped as part of _impute_missing_int_year.
        if not self.k.year.startswith('int_'):
            clip_values(self.k.year, Ranges.YEAR_RANGE)
        clip_values(self.k.month, Ranges.MONTH_RANGE)
        clip_values(self.k.day, Ranges.DAY_RANGE)

        return scratch

    def _calculate_age_columns(self, scratch):
        # Age in days
        if self._can_compute_age_day():  # or birth_interview_all:
            scratch[self.k.age_day] = self._age_days_from_birth_interview_timestamp(scratch)  # noqa

        # Age in months
        if self._can_compute_age_day():
            set_default_values(
                self.k.age_month,
                self._age_month_from_birth_interview_timestamp(scratch),
                scratch)
        if self._can_compute_age_month():
            set_default_values(
                self.k.age_month,
                self._age_month_from_birth_interview_year_month(scratch),
                scratch)

        # Age in years
        if self._can_compute_age_month():
            set_default_values(
                self.k.age_year,
                self._age_year_from_age_month(scratch),
                scratch)
        if self._can_compute_age_year():
            set_default_values(
                self.k.age_year,
                self._age_year_from_birth_interview_year(scratch),
                scratch)

        return scratch

    def _can_compute_age_day(self):
        """
        Can values can be found for {birth,int}_{year,month,day}?
        """
        # NOTE: int_year always present; can infer from survey start/end year
        i_month = i_day = b_year = b_month = b_day = False

        if self._configured(self.k.date):
            i_month = True
            if self._format_specifier_includes_day(
                    self.config[self.k.date_format]):
                i_day = True

        if self._configured(self.k.birth_date):
            b_year = b_month = True
            if self._format_specifier_includes_day(
                    self.config[self.k.birth_date_format]):
                b_day = True

        i_month |= self._configured(self.k.month)
        i_day |= self._configured(self.k.day)
        b_year |= self._configured(self.k.birth_year)
        b_month |= self._configured(self.k.birth_month)
        b_day |= self._configured(self.k.birth_day)

        return all([i_month, i_day, b_year, b_month, b_day])

    def _can_compute_age_month(self):
        # Provided directly?
        if self._configured(self.k.age_month):
            return True
        # Provided via age categoricals?
        if self._age_categorical_configured('month'):
            return True

        # compute from {birth,int}_{year,month}

        # is interview year + month provided? We can always impute int_year...
        have_int = self._configured(self.k.month)
        if self._configured(self.k.date, self.k.date_format):
            fmt = self.config[self.k.date_format]
            if self._format_specifier_includes_year(fmt) and \
                    self._format_specifier_includes_month(fmt):
                have_int = True

        # is birth year + month provided?
        have_birth = self._configured(self.k.birth_year, self.k.birth_month)
        if self._configured(self.k.birth_date, self.k.birth_date_format):
            fmt = self.config[self.k.birth_date_format]
            if self._format_specifier_includes_year(fmt) and \
                    self._format_specifier_includes_month(fmt):
                have_birth = True

        return have_int and have_birth

    def _can_compute_age_year(self):
        # provided directly?
        if self._configured(self.k.age_year):
            return True
        # derive from other values.  the minimum required are birth_year and
        # int_year. int_year is ALWAYS available as it is imputed if missing
        if self._configured(self.k.birth_year):
            return True
        # Provided via age categoricals? Note - age_month then seeds age_year
        if any(self._age_categorical_configured(X) for X in ('month', 'year')):
            return True

        if self._configured(self.k.birth_date, self.k.birth_date_format):
            fmt = self.config[self.k.birth_date_format]
            if self._format_specifier_includes_year(fmt):
                return True

        return False

    def _interview_month_configured(self):
        if self._configured(self.k.month):
            return True

        return self._configured(self.k.date, self.k.date_format)

    def _interview_day_configured(self):
        if self._configured(self.k.day):
            return True

        if self._configured(self.k.date, self.k.date_format):
            fmt = self.config[self.k.date_format]
            return self._format_specifier_includes_day(fmt)

        return False

    def _age_categorical_configured(self, ac_type):
        """
        Predicate: is age_categorical configured for return type `ac_type`?

        Args:
            ac_type: one of 'year' or 'month'.

        Returns bool indicating if age_categorical is configured and will
        generate a value for `ac_type`.
        """
        if not self._configured('age_categorical',
                                'age_categorical_parse'):
            return False
        type_ = self.config.get('age_categorical_type')

        return type_ == ac_type

    def _format_specifier_includes_year(self, fmt):
        # cmc provides year + month
        return is_SIF(fmt) or 'y' in fmt or fmt == 'cmc'

    def _format_specifier_includes_month(self, fmt):
        # Do not check for 'cmc' because it coincidentally has an 'm' in it
        return is_SIF(fmt) or 'm' in fmt

    def _format_specifier_includes_day(self, fmt):
        return is_SIF(fmt) or 'd' in fmt

    def _validate_results(self, scratch):
        raise_error = False
        for col_name in self.k.age_year, self.k.age_month, self.k.age_day:
            col = scratch[col_name]
            if col.isnull().all():
                continue
            negative = col < 0
            cnt = negative.sum()
            if cnt:
                ratio = cnt / len(negative)
                base = (f"{cnt} negative ages in {col_name} - "
                        f"{ratio:.2f} of whole.")
                if ratio < MAX_RATIO_NEGATIVE_AGE:
                    msg = f"{base} This is less than 2% - setting to NA"
                    self.logger.warning(msg)
                    scratch.loc[negative, col_name] = numpy.nan
                else:
                    msg = (f"{base} This is more than 2%. "
                           f"Check {col_name} and fix.")
                    self.logger.error(msg)
                    raise_error = True

        if raise_error:
            msg = "Too many invalid/negative values in age columns - must fix."
            raise RuntimeError(msg)

        return scratch

    def _merge_output_columns_into_df(self, df, scratch):
        """
        Merges scratch columns into result dataframe.
        """
        for col_name in self.k.columns():
            col = scratch[col_name]
            # Two conditions for merge:
            # already present e.g., int_date value are bad and were clipped
            # not present; have useable calculations
            if col_name in df.columns or not pandas.isnull(col).all():
                df[col_name] = col
        return df

    def _age_days_from_birth_interview_timestamp(self, scratch):
        "Calculate age in days via timestamp subtraction."
        birth_dt = DatetimeFactory(
            self.k.birth_year, self.k.birth_month, self.k.birth_day)
        interview_dt = DatetimeFactory(
            self.k.year, self.k.month, self.k.day)

        result = []
        for _, row in scratch.iterrows():
            birth = birth_dt(row)
            interview = interview_dt(row)
            try:
                delta_at_interview = interview - birth
            except TypeError:
                result.append(numpy.nan)
            else:
                if pandas.isnull(delta_at_interview):
                    result.append(numpy.nan)
                else:
                    result.append(delta_at_interview.days)

        return result

    def _age_month_from_birth_interview_timestamp(self, scratch):
        """
        Calculate age in months.

        Original code was written in Stata and relied on the mofd() function.

        Testing output from the first 10,000 days starting 1990-01-01 it
        appears that this function simply performs simple date math to return
        (year * 12 + month) - EPOCH, where the Stata epoch is 1960-01.
        """
        birth_dt = DatetimeFactory(
            self.k.birth_year, self.k.birth_month, self.k.birth_day)
        interview_dt = DatetimeFactory(
            self.k.year, self.k.month, self.k.day)

        def date_to_months(date):
            return (12 * date.year + date.month)

        result = []
        for _, row in scratch.iterrows():
            birth = birth_dt(row)
            if pandas.isnull(birth):
                result.append(numpy.nan)
                continue
            interview = interview_dt(row)
            if pandas.isnull(interview):
                result.append(numpy.nan)
                continue
            age_months = date_to_months(interview) - date_to_months(birth)
            result.append(age_months)
        return result

    def _age_month_from_birth_interview_year_month(self, scratch):
        birth_months = scratch[self.k.birth_year] * 12 + \
            scratch[self.k.birth_month]
        int_months = scratch[self.k.year] * 12 + scratch[self.k.month]
        age_months = int_months - birth_months
        return age_months

    def _age_year_from_age_month(self, scratch):
        return (scratch[self.k.age_month] / 12).round(decimals=2)

    def _age_year_from_birth_interview_year(self, scratch):
        return scratch[self.k.year] - scratch[self.k.birth_year]


# Helper methods
def coerce_column_to_numeric(series: pandas.Series):
    """
    Returns DataFrame column (series) with a numeric dtype.
    """
    if not is_numeric_dtype(series.dtype):  # ensure also numeric
        series = pandas.to_numeric(series, errors='coerce')
    return series


def is_SIF(fmt):
    "Does `fmt` refers to a Stata Internal Format (SIF) date?"
    return '%' in fmt


# Helper classes
class AgeKeys:
    """
    Namespaces identifier keys for demographics calculations.

    The default prefix is '', but both 'mother_' and 'child_' are used for the
    child_growth_failure and demographics_child research topics, respectively.

    The default date_type is 'int' for interview which is required in calculating age_year,
    age_month and age_day. However, the date_type is generic. e.g. it can be 'reg' for processing
    registration dates which is used by 'dem_vr' and 'dem_br_vr' custom topics.
    """
    def __init__(self, prefix, date_type='int'):
        self.age_year = f"{prefix}age_year"
        self.age_month = f"{prefix}age_month"
        self.age_day = f"{prefix}age_day"

        self.birth_date = f"{prefix}birth_date"
        self.birth_date_format = f"{self.birth_date}_format"
        self.birth_year = f"{prefix}birth_year"
        self.birth_month = f"{prefix}birth_month"
        self.birth_day = f"{prefix}birth_day"

        self.date_format = f"{date_type}_date_format"
        self.date = f"{date_type}_date"
        self.day = f"{date_type}_day"
        self.month = f"{date_type}_month"
        self.year = f"{date_type}_year"

    def columns(self):
        "Return 'scratch data' columns."
        # Note: does not yield int_date or birth_date
        yield self.age_year
        yield self.age_month
        yield self.age_day

        yield self.year
        yield self.month
        yield self.day

        yield self.birth_year
        yield self.birth_month
        yield self.birth_day


class DateExtractor:
    """
    Extracts year/month/day values from encoded fields.

    Use factory method from_format(fmt) to create this
    Use yield_date_parts(date_column) to provide year/month/day series.
    """
    @classmethod
    def from_format(cls, fmt):
        r"""
        Formats to support:
            "cmc" - produces year, month values. NO DAY
            any format with "%" in it - produces year, month, day values.
            ([^a-zA-Z\d\s:]| ) - MAY produce year, month, day
            [a-zA-Z\d\s:] - MAY produce year, month, day
        """
        # Date extraction is a pain point in the existing implementation. It is
        # hoped that this method will be adjusted to support additional formats
        if fmt.upper() == 'CMC':
            return cls('CMC')

        if is_SIF(fmt):
            return cls('SIF')  # https://www.stata.com/manuals13/ddatetime.pdf

        match = re.search(r'[^a-zA-Z\d\s:]| ', fmt)
        if match:
            # Delimiter is a space OR first non-space value in regex
            if ' ' in fmt:
                delimiter = ' '
            else:
                delimiter = re.escape(match.group())  # first matched character
            return cls('YMD-GROUPS', delimiter=delimiter, long_format=fmt)

        if re.search(r'[a-zA-Z\d\s:]', fmt):
            return cls('YMD-RANGES', long_format=fmt)

        # Can this even be reached without unicode or some *very* odd input?
        raise ValueError(f"Unknown format {fmt!r}")

    def __init__(self, fmt, **kwargs):
        self.fmt = fmt

        if fmt == 'CMC':
            self._inner_call = self.extract_cmc
        elif fmt == 'SIF':
            self._inner_call = self.return_datetime
        elif fmt == 'YMD-GROUPS':
            self._inner_call = self.extract_ymd_groups
            order = [X.lower()[0]
                     for X in re.split(kwargs['delimiter'],
                                       kwargs['long_format'])]
            self.order = order
            self.delimiter = kwargs['delimiter']
        elif fmt == 'YMD-RANGES':
            self._inner_call = self.extract_ymd_ranges

            lf = kwargs['long_format']
            ex_len = len(lf)
            self.slice_extractors = [
                IntSlice(start=lf.find(c),
                         end=lf.rfind(c),
                         expected_len=ex_len)
                for c in 'ymd']
        else:
            raise ValueError(f"Unknown extraction format {fmt!r}")

    def yield_date_parts(self, encoded_column):
        """
        Yields pandas.Series values for extracted year/month/day values.

        Args:
            encoded_column (pandas.Series): values to decode.

        Only yields values which are present. For example, Century Month Code
        does not contain day information so only a year and a month Series will
        be yielded.
        """
        try:
            df = pandas.DataFrame([self.extract_date_values(X)
                                   for X in encoded_column],
                                  columns=['year', 'month', 'day'])
        except Exception as e:
            import sys
            tb = sys.exc_info()[2]
            msg = f"{e.args[0]} for column {encoded_column.name}"
            raise errors.Error(msg).with_traceback(tb) from None
        for col_name in df:
            col = df[col_name]
            if not col.isnull().all():
                yield col

    def extract_date_values(self, value,
                            on_nan=(numpy.nan, numpy.nan, numpy.nan)):
        """
        Wrapper method to handle NaN values. Otherwise returns value from
        delegated method _inner_call, assigned in __init__.

        Returns 3 element iterable of numbers representing the
            (year, month, day)
        extracted from `value`. If any number cannot be extracted NaN is used.
        """
        if pandas.isnull(value):
            return on_nan

        return self._inner_call(value)

    # Methods to extract the supported formats.
    # One of these is assigned to _inner_call
    def extract_cmc(self, value):
        "Decodes and returns datetime.date objects for a Century Month Code."
        try:
            dt = decode_century_month_code(value)
        except ValueError:
            return (numpy.nan, numpy.nan, numpy.nan)
        else:
            # Century month code's do not include a day component
            return (dt.year, dt.month, numpy.nan)

    def return_datetime(self, value):
        """
        Returns the (year, month, day) values of a datetime object.

        This method would be named extract_stata_internal_format but pandas
        helpfully performs these conversions for us, so all that is left to be
        done is return a (year, month, day) tuple.
        """
        if not isinstance(value, dt.date):
            msg = f"Value {value!r} is not SIF or was incorrectly converted"
            raise TypeError(msg)
        return (value.year, value.month, value.day)

    def extract_ymd_groups(self, value):
        """
        Extracts year/month/day values from a string with delimiters.
        """
        # convert `value` to str (original implementation does this)
        pieces = re.split(self.delimiter, str(value))
        # in theory you'll get a dict of {'d': FOO, 'm': BAR, 'y': BAZ}
        lookup = dict(zip(self.order, pieces))
        # attempt to convert y/m/d format values to number
        for key in 'ymd':
            try:
                lookup[key] = int(lookup[key])
            except (KeyError, ValueError, TypeError):
                lookup[key] = numpy.nan

        return (lookup['y'], lookup['m'], lookup['d'])

    def extract_ymd_ranges(self, value):
        """
        Extracts year/month/day values from a str.Uses a pre-computed index.
        """
        if isinstance(value, float):
            value = int(value)  # drop floating point numbers before stringing
        value = str(value)  # on rare occasion value is non-str
        result = tuple(int_slice(value) for int_slice in self.slice_extractors)
        return result


@attrs_with_logger
class DatetimeFactory:
    """
    Produces datetime.datetime values on a per-row basis.
    """
    year_col = attrib()
    month_col = attrib()
    day_col = attrib()

    def __call__(self, row):
        """
        Attempts to calculate the date this row's subject was born.

        Args:
            row: object with self.{year,month,day}_col attributes.

        Returns datetime.datetime with only year/month/day data.
        """
        # this is caused by {birth,int}_{day,month,year} not being
        # present in indicators
        #
        # Currently they're hardcoded as input_meta in
        # winnower.config.models.ubcov which means the input is provided
        # as a literal value (instead of aliasing the column, which is
        # more typical)
        try:
            year = self._row_date_piece(self.year_col, row)
            month = self._row_date_piece(self.month_col, row)
            day = self._row_date_piece(self.day_col, row)
        except errors.NullError:
            return float('NaN')  # got a NaN - return a NaN

        template_err = (f"Error creating datetime with values year: "
                        f"{year!r} month: {month!r} day: {day!r} for "
                        f"{self.year_col}/{self.month_col}/{self.day_col}")
        try:
            return dt.datetime(year, month, day)
        except TypeError as e:
            # Unknown/unexpected error
            msg = f"{template_err} with err {e}"
            self.logger.error(msg)
            raise errors.Error(msg)
        except ValueError as e:
            if 'is out of range for' in e.args[0]:
                # Is this e.g., "day is out of range for month" (caused by
                # actual survey with June 31st value)? Return a NaN.
                msg = f"{template_err} with error message {e}"
                self.logger.warning(msg)
                return float('NaN')
            elif 'month must be in 1..12' in e.args[0]:
                # Is month not valid?
                msg = f"{template_err} with error message {e}"
                self.logger.warning(msg)
                return float('NaN')
            # Unknown/unexpected error
            msg = f"{template_err} with err {e}"
            self.logger.error(msg)
            raise errors.Error(msg)

    def _row_date_piece(self, col, row):
        """
        Return value suitable for argument to datetime.datetime.

        Args:
            col: name of the column with the date piece e.g., 'birth_year'.
            row: the row of data to get the date piece from.

        Raises:
            NullError: the value at row[col] is null (pandas.isnull())
            Error: the value at row[col] is a float that is non-integer (has
                   a fractional value)
                   OR
                   The value is not integer or float and raises an error when
                   int(value) is called to convert it.

        Returns: a valid int-like value for datetime constructor.
        """
        val = row[col]

        if pandas.isnull(val):  # trigger error in calling function
            raise errors.NullError()

        # convert to int - likely a float
        if is_float_value(val):
            rem, i = numpy.modf(val)
            if rem:
                msg = f"column {col} has non-integer value {val!r}"
                raise errors.Error(msg)
            return int(i)
        elif isinstance(val, (int, numpy.integer)):
            return val  # datetime.datetime will take int8, int16, ...
        else:
            try:
                val = int(val)
                return val
            except Exception as e:
                msg = (f"Error converting column {col} to an integer. "
                       f"Erroring value {val!r} and original error {e})")
                raise errors.Error(msg)


class IntSlice:
    """
    Returns int value for text in slice or NaN.

    Encapsulates logic in ubCov date extraction code.
    """
    def __init__(self, start, end, expected_len):
        self.start = start
        self.end = end
        self.expected_len = expected_len

    def __repr__(self):
        # !r forces the f-string to use __repr__ instead of __str__.
        return (f"IntSlice(start={self.start!r}, "
                f"end={self.end!r}, "
                f"expected_len={self.expected_len!r})")

    def __call__(self, val):
        if self.start == -1:
            return numpy.nan
        # original code 0-pads val if short. Instead adjust slice index
        val_is_short = len(val) < self.expected_len
        i = max(self.start - 1, 0) if val_is_short else self.start
        j = self.end + (0 if val_is_short else 1)
        try:
            return int(val[i:j])
        except ValueError:
            # e.g., "invalid literal for int() with base 10: 'jan'
            return numpy.nan


class RangesBase:
    """
    Class for range constants for demographics calculations.

    Namedspaced with a "Base" suffix so I can immediately instantiate it as a
    class singleton.
    """
    AGE_YEAR_RANGE = 0, 120
    AGE_MONTH_RANGE = 0, 1440
    AGE_DAY_RANGE = 0, 43400

    MONTH_RANGE = 1, 12
    DAY_RANGE = 1, 31

    @property
    def YEAR_RANGE(self):
        # quacks like an attribute
        return 1900, dt.datetime.now().year


Ranges = RangesBase()

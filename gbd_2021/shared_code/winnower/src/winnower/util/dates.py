import datetime as dt
import re

import pandas

from winnower import errors
from winnower.util.dtype import is_float_value
from ethiopian_date import EthiopianDateConverter
import nepali_datetime
from persiantools.jdatetime import JalaliDate


def decode_century_month_code(code) -> dt.date:
    """
    Decode century month code (CMC) into a date object.

    CMC is an encoding scheme used by DHS Surveys.

    http://www.dhsprogram.com/pubs/pdf/DHSG4/Recode4DHS.pdf

    Technically the code refers to a month. Since it is ambiguous anyway, this
    function will always return a date referring to the first of the month.
    """
    if isinstance(code, str):
        if not code.isdigit():
            raise ValueError(f"Non-digit code {code!r} provided")
        code = int(code)
    elif is_float_value(code):
        if not code.is_integer():
            raise ValueError(f"Float value with decimal {code!r} provided")
        code = int(code)
    years, months = divmod(code, 12)
    if not months:  # handle December
        months = 12
        years -= 1

    return dt.date(1900 + years, months, 1)


def encode_century_month_code(year, month) -> int:
    """
    Encode century month code (CMC) into a date object.

    CMC is an encoding scheme used by DHS Surveys.

    http://www.dhsprogram.com/pubs/pdf/DHSG4/Recode4DHS.pdf
    """
    r = (year - 1900) * 12 + month
    return r


def datetime_to_numeric(dates_col):
    """
    Converts datetime values to numeric values.

    These values represent the number of days since 1960-01-01.

    This is the expected value you would get if you took a Stata date column
    that has the %td format and copied it without the format.

    See also STATA docs - https://www.stata.com/manuals13/u24.pdf
    """
    # This calculation is very specific to stata dates stored as longs in the
    # original data file. As winnower is meant to mimic ubCov behavior and so
    # stata behavior the intent of creating a continuous column from a date was
    # likely meant to create a column of type long which represents the days
    # since jan, 1 1960 or if datetime, the milliseconds since that date.
    # However, for this to be generally useful more information is needed in
    # the configuration for this column which specifies how this date or
    # datetime should be converted.
    if isinstance(dates_col, pandas.Series):
        try:
            col = (dates_col.dt.date - dt.date(1960, 1, 1)).dt.days
        except AttributeError:
            # in an older version of pandas an OutOfBoundsDatetime was raised
            # here but this behavior has changed. As such, pray actual
            # datetimes were passed and error if not.
            epoch = dt.datetime(1960, 1, 1)
            NaN = float('NaN')
            isnull = pandas.isnull
            # Note: must explicitly convert NaT to NaN
            values = [NaN if isnull(X) else (X - epoch).days
                      for X in dates_col]
            col = pandas.Series(values, index=dates_col.index)
    elif isinstance(dates_col, (pandas.Timestamp, dt.datetime)):
        col = (dates_col - dt.datetime(1960, 1, 1)).days
    else:
        msg = f"Provide datetime-like object or series not {dates_col!r}"
        raise errors.Error(msg)
    # The above works for dates but it is impossible to determine from the col
    # if it is a datetime or date only value. It is likely that what is needed
    # is an accessor on the datetime[ns] col that will allow for conversion
    # back to the original stata value ... or don't have read_stata convert
    # dates and instead add an accessor that allows for retrieving a date or
    # datetime value from the col.
    return col


def date_extractor_from_format(fmt):
    """
    Return appropriate DateValExtractor for the given format.
    """
    # Determine which kind of parser we use based on regex.
    # This regular expression probably doesn't make sense given it determines
    # whether a format is "delimited" or not. Modeled after ubCov...
    # <ADDRESS>
    match = re.search(r'([^a-zA-Z\d\s:]| )', fmt)
    if match:
        delimiter = match.group()
        return UbcovDelimitedDateExtractor(delimiter, fmt)
    else:
        return UbcovDateExtractor(fmt)


class DateValExtractor:
    """Common code for DateExtractor's."""
    def _get_day(self, val):
        return self._check_and_extract_val(val, 'day', 1, 31)

    def _get_month(self, val):
        return self._check_and_extract_val(val, 'month', 1, 12)

    def _get_year(self, val):
        return self._check_and_extract_val(val, 'year')

    def _check_and_extract_val(self, val, label, min=None, max=None):
        if not val.isdigit():
            raise ValueError(f"Value {val!r} not suitable as {label}")
        val = int(val)
        if min and val < min:
            raise ValueError(f"Value {val} less than logical minimum {min}")
        if max and val > max:
            raise ValueError(f"Value {val} greater than logical maximum {max}")
        return val


class UbcovDelimitedDateExtractor(DateValExtractor):
    """
    Date parsing/extracting based off of ubCov's split_date.

    Args:
        delimiter: delimiter which separates date variables.
        format: format template to extract values.

    Example:
        UbcovDelimitedDateExtractor('-', 'yyyy-mm-dd')
        UbcovDelimitedDateExtractor('/'. 'mm/dd/yy')
    """
    def __init__(self, delimiter, format):
        # NOTE: depends on Python 3.6's dict being ordered. This is tested
        self.parts = {}
        for part in format.replace(delimiter, ' ').split():
            c = part.lower()[0]
            if c == 'd':
                self.parts['day'] = self._get_day
            elif c == 'm':
                self.parts['month'] = self._get_month
            elif c == 'y':
                self.parts['year'] = self._get_year
            else:
                msg = f"Unexpected code {c!r} - expected 'y', 'm', or 'd'"
                raise ValueError(msg)

        if not self.parts:
            msg = f"Format {format} did not contain 'd', 'm', or 'y'"
            raise ValueError(msg)

        self.valid_attrs = frozenset(self.parts)
        self.delimiter = delimiter
        self.format = format

    def __call__(self, val):
        kw = {
            'day': 1,
            'month': 1,
            'year': 1,
        }
        pieces = val.split(self.delimiter)

        if len(pieces) != len(self.parts):
            msg = f"Value {val} does not match format {self.format}"
            raise ValueError(msg)

        for piece, (date_part, extractor) in zip(pieces, self.parts.items()):
            kw[date_part] = extractor(piece)
        return dt.date(**kw), self.valid_attrs


class UbcovDateExtractor(DateValExtractor):
    """
    Extracts date values from strings.

    Args:
        format: string containing expected date format. E.g., 'yyyymmdd'

    Provide 'yyyy' (or 'yy') 'mm' and/or 'dd' values in the format string.
    Other characters will be ignored.
    """
    _datepart_subformat = (
        ('day', 'dd'),
        ('month', 'mm'),
        # order of year matters
        ('year', 'yy'),
        ('year', 'yyyy'),
    )

    def __init__(self, format):
        """
        Create parser for format of y's, m's, d's.

        Example formats:
            'mmddyyyy'
            'ddmmyy'
            'mmyyyy'
            'mmyy'
            'ddmm'
        """
        self.parts = {}
        for date_part, fmt in self._datepart_subformat:
            match = re.search(fmt, format)
            if match is not None:
                self.parts[date_part] = slice(*match.span())

        if not self.parts:
            msg = f"Format {format} did not contain dd, mm, yy, or yyyy"
            raise ValueError(msg)

        self.valid_attrs = frozenset(self.parts)
        self.format = format

    def __call__(self, val):
        """
        Extract day/month/year values from `val`
        """
        kw = {
            'day': 1,
            'month': 1,
            'year': 1,
        }
        for date_part, extractor_slice in self.parts.items():
            substr = val[extractor_slice]
            if not substr:
                msg = f"Value {val} does not match format {self.format}"
                raise ValueError(msg)

            kw[date_part] = getattr(self, f'_get_{date_part}')(substr)

        return dt.date(**kw), self.valid_attrs


class DateConverter:
    """
    Converts non-standard date e.g. Nepali (NPL), Ethiopian (ETH)) to standard
    Gregorian date.

    Args:
        calendar_type: 3-letter country-code that specifies the type of
            non-standard calendar.
        df: pandas dataframe which contains non-standard date.
        year: non-standard date year column.
        month: non-standard date month column.
        day: non-standard date day column. If the day column is not provided
            it assumes it to be the first day of the month.
    """
    @classmethod
    def to_gregorian(cls, calendar_type, df,
                     year, month, day=None):
        tg = cls(calendar_type, df,
                 year, month, day)

        if year not in df or month not in df:
            msg = f"Column/s not in df check: year: {year} month: {month}"
            raise errors.ValidationError(msg)

        if day and day not in df:
            msg = f"day column not in df check: {day}"
            raise errors.ValidationError(msg)

        if day:
            df[[year, month, day]] = df.apply(tg.convert_date, axis=1, result_type="expand")  # noqa
        else:
            df[[year, month]] = df.apply(tg.convert_date, axis=1, result_type="expand")  # noqa

        return df

    def __init__(self, calendar_type, df, year, month, day):
        self.calendar_type = calendar_type
        self.df = df
        self.year = year
        self.month = month
        self.day = day

    def convert_date(self, row):
        """
        Calls for specific calendar conversion method
        Args:
            row: Dataframe row passed from df.apply()
        """
        if self.calendar_type == 'NPL':
            # NPL: Nepal
            return self._nepali_date_to_gregorian(row)
        elif self.calendar_type == 'ETH':
            # ETH: Ethiopia
            return self._eth_date_to_gregorian(row)
        elif self.calendar_type == 'AFG' or self.calendar_type == 'IRN':
            # AFG: Afganistan, IRN: Iran
            return self._jalali_date_to_gregorian(row)
        else:
            msg = f"Calendar type {self.calendar_type} not supported yet."
            raise NotImplementedError(msg)

    def _nepali_date_to_gregorian(self, row):
        """
        Converts Nepali date to standard Gregorian date.
        If day column is not provided assumes first day of the month.
        Args:
            row: Dataframe row passed from df.apply()
        """
        if not self.day:
            nepali_date = nepali_datetime.date(row[self.year], row[self.month], 1)  # noqa
            g_date = nepali_date.to_datetime_date()
            return g_date.year, g_date.month
        else:
            nepali_date = nepali_datetime.date(row[self.year], row[self.month], row[self.day])  # noqa
            g_date = nepali_date.to_datetime_date()
            return g_date.year, g_date.month, g_date.day

    def _eth_date_to_gregorian(self, row):
        """
        Converts Ethiopian date to standard Gregorian date.
        If day column is not provided assumes first day of the month.
        Args:
            row: Dataframe row passed from df.apply()
        """
        conv = EthiopianDateConverter.to_gregorian
        if not self.day:
            g_date = conv(row[self.year], row[self.month], 1)
            return g_date.year, g_date.month
        else:
            g_date = conv(row[self.year], row[self.month], row[self.day])
            return g_date.year, g_date.month, g_date.day

    def _jalali_date_to_gregorian(self, row):
        """
        Converts Jalali date to standard Gregorian date. The variants of
        Jalali (solar) calendar are used in Iran and Afganistan and they
        differ in month names but numeric dates are the same.
        The one implemented here is Shamsi version for Iran.
        If day column is not provided assumes first day of the month.
        Args:
            row: Dataframe row passed from df.apply()
        """
        if not self.day:
            g_date = JalaliDate(row[self.year], row[self.month], 1).to_gregorian()  # noqa
            return g_date.year, g_date.month
        else:
            g_date = JalaliDate(row[self.year], row[self.month], row[self.day]).to_gregorian()  # noqa
            return g_date.year, g_date.month, g_date.day

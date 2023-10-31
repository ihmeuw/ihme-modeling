"""
The dem_vr module.

following direction here: <ADDRESS>

Using google doc here for configuration: <REDACTED>
"""
from .base import TransformBase
from winnower.util.dtype import is_categorical_dtype, is_integer_dtype, is_float_dtype
from winnower.util.categorical import category_codes
from winnower.util.dataframe import get_column_type_factory
from winnower import errors
from winnower.util.stata import is_missing_str
from winnower.util.age_calculator import (
    DateExtractor,
    AgeCalculator,
    AgeKeys
)
from numpy import safe_eval


class Transform(TransformBase):
    """
    Processes dem_vr inputs to provide output column age_of_death_units.

    Map incoming data (either categorical codes or text objects) to string output # noqa
    """
    def __init__(self, columns, config, extraction_metadata):
        super().__init__(columns, config, extraction_metadata)
        # date_type='reg' is to process reg_date and output (reg_year, reg_month, reg_day) columns
        self.keys = AgeKeys(prefix='', date_type='reg')
        self.reg_date_calc = AgeCalculator(columns, config, self.keys)

    def output_columns(self, input_columns):
        "Add columns to the list of output_columns."
        output_columns = list(input_columns)
        output_columns.extend(['aod_number', 'age_of_death_units'])

        # Add columns to process 'reg_date'.
        output_columns.extend(col for col in self.reg_date_calc.calculated_columns() if col not in input_columns)

        return(output_columns)

    def validate(self, input_columns):
        # if 'aod_format' and 'aod_combined' are present,
        #  'aod_unit_mapping' must be present
        format = self.config.get('aod_format')
        aod_comb = self.config.get('aod_combined')

        if format and aod_comb:
            # validate 'aod_format'
            if 'u' not in format or 'n' not in format:
                msg = ("'u' for units and 'n' for age "
                       f"required in aod_format: {format}")
                raise errors.ValidationError(msg)

            # check for misconfiguration e.g. 'uunun'
            sum_position = format.rfind('u') - format.find('u') + \
                format.rfind('n') - format.find('n') + 2
            if sum_position != len(format):
                msg = ("aod_format is not configured properly: "
                       f"(u)nit and (n)umber codes must be contiguous - {format}") # noqa
                raise errors.ValidationError(msg)

            format_copy = format.replace('u', '')
            format_copy = format_copy.replace('n', '')
            if format_copy:
                msg = ("Only 'u' for units and 'n' for age accepted "
                       f"in aod_format, extra: {format_copy}")
                raise errors.ValidationError(msg)

            # validate aod_unit_mapping
            self.aod_map_dict = self.get_aod_map_dict(self.config.get('aod_unit_mapping'))

        # If only one of format or aod_comb present, error
        elif (format is None and aod_comb) or (format and aod_comb is None):
            isNone = "aod_format" if aod_comb else "aod_combined"
            notNone = "aod_format" if aod_comb is None else "aod_combined"
            msg = (f"{isNone} is missing and {notNone} is filled out. "
                   "Both expected to be either filled out or empty.")
            raise errors.ValidationError(msg)
        elif self.config.get('aod_units') and self.config.get('aod_number'):
            self.aod_map_dict = self.get_aod_map_dict(self.config.get('aod_unit_mapping'))
        else:
            msg = "Neither aod_format and aod_combined, or aod_units and aod_number is present.\n"
            self.logger.info(msg)

    def get_aod_map_dict(self, aod_unit):
        """
        Read in the map dictionary from the aod_unit_mapping column
        """

        if aod_unit is None:
            msg = ("aod_unit_mapping not filled in but age_of_death_units "
                   "or age_of_death_combined is.")
            raise errors.ValidationError(msg)
        else:
            # parse and validate 'aod_unit_mapping'
            try:
                map_dict = safe_eval(aod_unit)
            except Exception as e:
                msg = (f"Error while converting aod_unit_mapping string "
                       f"to dictionary.\n{e}")
                raise errors.ValidationError(msg)
            else:
                nan = float('NaN')
                # replace string NaN (or nan, or...) with float('NaN') in dict
                for key, val in map_dict.items():
                    if val.lower() == 'nan':
                        map_dict[key] = nan

            if not isinstance(map_dict, dict):
                msg = (f"Expected dict type for aod_unit_mapping : "
                       f"{map_dict}, was passed of type {type(map_dict)}")
                raise errors.ValidationError(msg)

            return(map_dict)

    def execute(self, df):
        """
        First check to see if column name is present, replace stata missing with nan, # noqa
        then get modified data frame
        """
        # if 'aod_format' and 'aod_combined' columns present, separate
        #  'aod_combined' into 'aod_number' and 'age_of_death_units'
        if self.config['aod_format'] and self.config['aod_combined']: # noqa
            df = self.separate_aod_and_unit(df)
        # If aod_units present add age_of_death_units column
        elif self.config['aod_units'] and self.config.get('aod_number'): # noqa
            col_name = self.get_column_name(self.config['aod_units'])
            df = self.aod_units(col_name, df)
            df['aod_number'] = df[self.get_column_name(self.config['aod_number'])].copy()
        else:
            msg = "Neither aod_format and aod_combined, or aod_units and aod_number is present.\n"
            self.logger.debug(msg)

        # If date_of_death present do WINVERSE-143 processing
        if self.config['date_of_death']:
            col_name = self.get_column_name(self.config['date_of_death'])
            df = self.date_of_death(col_name, df)

        df = self.reg_date_calc.calculate_ages(df)
        return(df)

    def replace_stata_missing(self, col_name, df):
        # replace stata is_missing_str value with NaN, don't do it in place # noqa
        temp_col = df[col_name].copy()
        for val in temp_col.unique():
            if is_missing_str(val):
                temp_col = temp_col.replace(val, float('nan'))
        df[col_name] = temp_col
        return(df)

    def aod_units(self, col_name, df):
        """
        add age_of_death_units column based on dictionary from aod_unit_mapping
        """
        df = self.replace_stata_missing(col_name, df)

        df['age_of_death_units'] = self._get_aodu_col(df, col_name)

        return(df)

    def check_string_lengths(self, format, col):
        "raise error for input data strings that are malformed"
        col = col.dropna()
        bad = col.str.len() != len(format)
        if bad.any():
            err_msg = ""
            to_report = col[bad[bad].index]  # awkward calling bad[bad]
            for idx, val in to_report[:5].iteritems():
                err_msg += f"aod_format: '{format}' does not match aod_combined value '{col[idx]}' on row {idx}.\n"

            n_bad = len(to_report)
            if n_bad > 5:
                err_msg += f"this occurs on another {n_bad - 5} rows"

            raise errors.ValidationError(err_msg)

    def separate_aod_and_unit(self, df):
        """
        If 'aod_format' and 'aod_combined' are present, this function separates
        the aod_combined column into 'aod_number' and 'age_of_death_units' by
        mapping it to 'aod_unit_mapping'.
        """
        # get the columns required for decoding
        num_unit_col = self.config['aod_combined']

        df = self.replace_stata_missing(num_unit_col, df)
        format = self.config['aod_format']
        mask = df[num_unit_col].notna()

        # Collect start-end indices for units and ages
        unit_start = format.find('u')
        unit_end = format.rfind('u')+1
        age_start = format.find('n')
        age_end = format.rfind('n')+1

        if is_integer_dtype(df[num_unit_col]):
            df.loc[mask, num_unit_col] = df[num_unit_col].astype(str)
        elif is_float_dtype(df[num_unit_col]):
            df.loc[mask, num_unit_col] = df[num_unit_col].astype(int).astype(str)

        self.check_string_lengths(format, df[num_unit_col])

        units_col = df.loc[mask, num_unit_col].str[unit_start:unit_end]
        input_keys_set = set(units_col.dropna().unique())
        self._check_input_keys(input_keys_set, set(self.aod_map_dict.keys()))

        df['age_of_death_units'] = units_col.replace(self.aod_map_dict)
        df['aod_number'] = df.loc[mask, num_unit_col].str[age_start:age_end].astype(int)

        return(df)

    def date_of_death(self, col_name, df):
        """
        Return dataframe with columns death_year, death_month, death_day added
        """
        col = df[col_name]

        # create dictionary to replace missing with nan in dataframe
        nan = float('NaN')
        type_func = get_column_type_factory(col)
        null_map = {type_func(val): nan for val in self.config['date_of_death_missing']} # noqa
        col = col.replace(null_map)

        de = DateExtractor.from_format(self.config['date_of_death_format'])
        for temp_col in de.yield_date_parts(col):
            temp_col.index = df.index  # update index before assignment to df
            temp_col.name = f"death_{temp_col.name}"
            df[temp_col.name] = temp_col
        return(df)

    def _check_input_keys(self, input_keys_set, map_keys_set):
        "Error if any input data set keys are not present in map set from configuration" # noqa

        err_msg = ""
        for bad_key in (input_keys_set - map_keys_set):
            err_msg += f"dem_vr, Key {bad_key} in input data is not in mapped set {map_keys_set}\n" # noqa

        if err_msg:
            raise errors.RequiredInputsMissing(err_msg)

    def _get_aodu_col(self, df, col_name):
        """
        Return column with replacement strings for indexes defined by google doc # noqa

        Remove empty columns from dictionary used to map column names to output values and warn # noqa
        Make dictionary to apply to data frame column and then apply it to create new column # noqa

        """
        map_dict = self.aod_map_dict
        nan = float('NaN')
        col = df[col_name]

        # get the set of input keys to compare to keys we are mapping
        input_keys_set = set(col.dropna().unique())

        # always replace nan with nan, create dictionary to map in dataframe
        d_map = {
            nan: nan,
        }

        # if any of your config values are present then we're using labels, otherwise it's indexes # noqa
        got_labels = any(val in map_dict.values() for val in input_keys_set)

        # use column_type_factory to set keys in map dict
        type_func = get_column_type_factory(col, use_category_dtype=got_labels)
        for key, val in map_dict.items():
            converted = type_func(key)
            d_map[converted] = val

        # if using categorical codes replace in col, update input_keys_set to use codes # noqa
        if is_categorical_dtype(col) and not got_labels:  # e.g. ubcov_id 2160, google doc line 4 # noqa
            col = category_codes(col)
            input_keys_set = set(col.dropna().unique())

        # throw error for missing keys
        self._check_input_keys(input_keys_set, set(d_map.keys()))

        return(col.map(d_map))

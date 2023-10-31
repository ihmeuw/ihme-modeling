from argparse import Namespace

from boltons.iterutils import partition

import pandas
import numpy as np

from winnower.util.categorical import (
    to_Categorical,
)
from winnower.config.models.setup_forms import IndicatorForm
from winnower.transform.indicators import transform_from_indicator
from winnower.util.dates import encode_century_month_code
from winnower.util.dataframe import set_default_values

from .base import TransformBase

from winnower.globals import eg  # Extraction context globals.

# For module level logging.
# logger = logging.getLogger(__loader__.name)


class Transform(TransformBase):

    def output_columns(self, input_columns):
        """
        Append extra_columns to input_columns and return it.
        """
        res = input_columns
        extra_columns = []

        if self.config['dod_other_format']:
            extra_columns.append('dod_other_format')

        extra_columns.extend(['sex_id', 'line_id', 'int_month',
                              'age_of_death_units', 'age_of_death_number',
                              'cmc_death_date'])

        if 'birth_year' in input_columns and 'birth_month' in input_columns:
            extra_columns.append('child_dob_cmc')

        if self.config["nid"] in (10001, 237943):
            extra_columns.append('int_month')

        if "mother_birth_month" in input_columns and \
                "mother_birth_year" in input_columns:
            extra_columns.append("mother_dob_cmc")

        if "child_dob_cmc" in input_columns and \
                "interview_date_cmc" in input_columns:
            extra_columns.append('birthtointerview_cmc')

        res.extend(X for X in extra_columns if X not in input_columns)
        return res

    def execute(self, df):

        # //////////////////////////////////
        # // Custom Indicator Generation
        # //////////////////////////////////

        # Original cbh.do comment:
        # Generate any existing variables which require custom code
        # For continuous variables (the missing values but be names
        # `varname'_missing

        df = self._generate_continuous_variables(df)
        # If True then log more detailed debugging info.
        if eg.more_debug:
            self.logger.debug(f"After generate continuous, shape: {df.shape}")

        # Original cbh.do comment:
        # the following section was modified by bstrub - population and
        # fertiltiy needs all women, not just women with children so I'm
        # adding an if statement that subsets to only women with children
        # if you're not me. I only added the if statement -- the "drop
        # if child_alive == ." was original.
        #
        # population/fertility hack (`drop if child_alive == .`)
        #   TODO: how do we support this toggle?

        if eg.more_debug:
            self.logger.debug("After keep only known child, "
                              f"shape: {df.shape}")

        # Original cbh.do comment:
        # For string variabels which you want to be the meta
        #
        # Calculate dod_other_format (TODO: why is this not an indicator?)
        #   process dod_other_format
        df = self._process_dod_other_format(df)
        if eg.more_debug:
            self.logger.debug("After process dod_other_format, "
                              f"shape: {df.shape}")

        # //////////////////////////////////
        # // Recoding
        # //////////////////////////////////

        # Original cbh.do comment:
        # Label the binary variables
        #
        # Label child_alive if present
        df = self._label_child_alive(df)

        # Original cbh.do comment:
        # --NONE--
        #
        # label sex_id
        df = self._label_sex_id(df)

        # Original cbh.do comment:
        # If line ID doesnt exist make it the row number
        #
        # if line_id does not exist, create (line_id == row number)
        df = self._ensure_line_id_exists(df)

        # Original cbh.do comment:
        # Generate child dob_cmc
        #
        # calculate child_dob_cmc
        df = self._generate_child_dob_cmc(df)

        # Original cbh.do comment:
        # Add the interview month if it is not in the dataset but in
        # the survey documentation
        #
        # Survey Specific
        if self.config["nid"] in (10001, 237943):
            """
            if "$nid" == "10001"{
                gen int_month = 5 // This survey was undertaken between 10th April and 31st May, cannot link to dataset so end month selected
            }
            """  # noqa

            df["int_month"] = 5

        # Original cbh.do comment:
        # If no interview month available impute midpoint of survey
        #
        # if int_month does not exist impute int_month
        df = self._impute_int_month(df)

        # Original cbh.do comment:
        # Generate interview_date_cmc from month and year output/imputed
        # values
        # 21Oct21 - requested
        df = self._generate_interview_date_cmc(df)

        # Original cbh.do comment:
        # Drop unwanted variables
        #
        df = self._drop_temporary_variables(df)

        # Original cbh.do comment:
        # --NONE--
        #
        # rename age_year child_age_year
        df.rename(columns={'age_year': 'child_age_year'}, inplace=True)

        # Original cbh.do comment:
        # format children age-at-death columns
        #

        #   TODO: re-factor Continuous.execute's destring() implementation
        """
        cap destring child_age_at_death_raw, replace
        """
        return df

    def _generate_continuous_variables(self, df):
        # here we generate several columns in a manner that appears to mirror
        # regular Continuous ("cont") indicator generation.
        columns = ['aod_months', 'aod_raw', 'aod_number',
                   'death_month', 'death_year', 'dod', 'time_lived_days',
                   'time_lived_months', 'time_lived_years']

        def is_configured(col):
            return self.config.get(col) is not None

        configured, unconfigured = partition(columns, key=is_configured)

        for column in unconfigured:
            df[column] = float('NaN')

        for column in configured:
            ind_config = {
                column: self.config[column],
                # key may be missing; default to None
                f'{column}_missing': self.config.get(f'{column}_missing'),
                'ubcov_id': self.config['ubcov_id'],
            }
            transform = self._manual_indicator(column, 'cont', ind_config)
            transform.validate(input_columns=df.columns)
            df = transform.execute(df)
            # TODO: generalize process used for Binary creation and do it here
            # as well!
#            indicator = Continuous(
#                input_column=self.config[column],
#                missing=self.config[f"{column}_missing"],
#                output_column=column,
#                code_custom=False,
#                map_indicator=False,
#                required=False)
#            df = indicator.execute(df)

        return df

    def _manual_indicator(self, ind_name, ind_type, ind_config):
        ind = IndicatorForm(indicator_name=ind_name,
                            topic_name='cbh',
                            indicator_type=ind_type)
        config = Namespace(**ind_config)
        transform = transform_from_indicator(ind, config, label_helper=None)
        return transform

    def _process_dod_other_format(self, df):
        dod_other_format = self.config['dod_other_format']
        if dod_other_format:
            """
            if !mi("$dod_other_format") {
                gen dod_other_format = "$dod_other_format"
            }
            """
            df['dod_other_format'] = dod_other_format
        else:
            """
            else {
                gen dod_other_format = .
            }
            """
            df['dod_other_format'] = np.nan
        return df

    def _calculate_age_at_death_months(self, df):
        """
        Calculates age_at_death_months from a variety of different sources.
        """
        def no_months():
            "Convenience function for building masks."
            return df.child_age_at_death_months.isna()

        def no_death_year():
            return df.death_year.isna()

        """
        /// Create age at death from units and number variables
        replace child_age_at_death_months = floor(age_of_death_number/30) if age_of_death_units == 1 & child_age_at_death_months == . & age_of_death_units !=.
        replace child_age_at_death_months = age_of_death_number if age_of_death_units  == 2  & child_age_at_death_months == . & age_of_death_units !=.
        replace child_age_at_death_months = age_of_death_number*12 + 6 if age_of_death_units == 3 & child_age_at_death_months == . & age_of_death_units !=.
        replace child_age_at_death_months = age_of_death_number/4.45 if age_of_death_units == 4 & child_age_at_death_months == . & age_of_death_units !=.
        """  # noqa
        units = df.age_of_death_units
        # compute from days
        df.loc[(units == 1) & no_months(),
               'child_age_at_death_months'] = df.age_of_death_number // 30
        # compute from months
        df.loc[(units == 2) & no_months(),
               'child_age_at_death_months'] = df.age_of_death_number
        # compute from years; use mean of 6 months into the year
        df.loc[(units == 3) & no_months(),
               'child_age_at_death_months'] = df.age_of_death_number * 12 + 6
        # compute from weeks. no idea why 4.45 is the divisor
        df.loc[(units == 4) & no_months(),
               'child_age_at_death_months'] = df.age_of_death_number / 4.45

        """
        // Create age at death from date of death
        gen cmc_death_date = 12*(death_year-1900)+death_month if child_age_at_death_months == . & death_year != .
        if !mi("$child_dob_cmc"){
            replace child_age_at_death_months = cmc_death_date - child_dob_cmc if child_age_at_death_months == . & death_year != .
        }
        """  # noqa
        df["cmc_death_date"] = 12 * (df.death_year - 1900) + df.death_month
        if 'child_dob_cmc' in df.columns:
            df.loc[no_months() & no_death_year(), 'child_age_at_death_months'] = df.cmc_death_date - df.child_dob_cmc  # noqa

        """
        // Create age at death months if given as days AND months AND years
        if !mi("$time_lived_years") & !mi("$time_lived_months"){
            replace child_age_at_death_months = cond(missing(time_lived_days), 0, time_lived_days/30) + cond(missing(time_lived_months), 0, time_lived_months) + cond(missing(time_lived_years), 0, time_lived_years*12) if child_age_at_death_months == .
            replace child_age_at_death_months = . if time_lived_days == . & time_lived_months == . & time_lived_years == .
        }
        """  # noqa
        if self.config['time_lived_years'] is not None and \
                self.config['time_lived_months'] is not None:
            lived_months = (df.time_lived_days.fillna(0) / 30) + \
                df.time_lived_months.fillna(0) + \
                (df.time_lived_years.fillna(0) * 12)
            df.loc[no_months(), 'child_age_at_death_months'] = lived_months
            # this correctly transcribes the previous code, but looks wrong.
            # it sets child_age_at_death_months to NaN IFF there is no
            # time_lived_years/months/days. Even if the value was already set..

            def time_lived_all_isna():
                return df.time_lived_days.isna() & df.time_lived_months.isna() & df.time_lived_years.isna() # noqa

            df.loc[df.time_lived_days.isna() & df.time_lived_months.isna() & df.time_lived_years.isna(),  # noqa
                   'child_age_at_death_months'] = float('NaN')

        """
        ////// Add any more formats  to change for age at death in here///////
        if !mi("$dod") & "$dod_other_format" == "dd/mm/yyyy" {
            split $dod, parse("/") gen(temp)
            destring temp2, replace
            destring temp3, replace
            replace temp2 = . if temp2 >12
            replace temp3 = . if temp3 >3000
            replace cmc_death_date = 12*(temp3-1900)+temp2
            replace child_age_at_death_months = cmc_death_date - child_dob_cmc
        }
        """
        if self.config['dod'] and \
                self.config['dod_other_format'] == 'dd/mm/yyyy':
            temp = df['dod'].str.split('/', expand=True)
            temp = temp.apply(pandas.to_numeric)
            temp.loc[temp[1] > 12, 1] = float('NaN')
            temp.loc[temp[2] > 3000, 1] = float('NaN')
            df['cmc_death_date'] = 12 * (temp[2] - 1900) + temp[1]
            df['child_age_at_death_months'] = \
                df.cmc_death_date - df.child_dob_cmc

        return df

    def _label_child_alive(self, df):
        if 'child_alive' not in df:
            return df

        df['child_alive'] = to_Categorical(df.child_alive, ['No', 'Yes'])
        return df

    def _label_sex_id(self, df):
        """
        // Label the binary variables
        label define YN 0 "No" 1 "Yes"
        label define sexMF  1"Male" 2"Female"
        """
        if 'sex_id' in df.columns:
            df['sex_id'] = to_Categorical(df.sex_id, {1: 'Male', 2: 'Female'})
        return df

    def _generate_child_dob_cmc(self, df):
        """
        Generates child's date of birth in CMC format if possible.

        If no birth month information is given sets the child's birth month to
        June.
        """

        if 'birth_year' not in df:
            self.logger.info("no year, can't generate child_dob_cmc")
            return df

        if 'birth_month' not in df:
            self.logger.info("no birth_month ... using year midpoint")
            self.logger.warning("child_dob_cmc used year midpoint")
            cmc = [encode_century_month_code(year, 6)
                   for year in df.birth_year]
        else:
            cmc = [encode_century_month_code(year, month)
                   for year, month in
                   df[['birth_year', 'birth_month']].itertuples(index=False)]

        if self.config['year_adjust'] and self.config['month_adjust']:
            self.logger.debug("generating child_dob_cmc, replace all")
            set_default_values('child_dob_cmc', cmc, df)
        else:
            self.logger.debug("generating child_dob_cmc, replace nans")
            set_default_values('child_dob_cmc', cmc, df, mask=np.ma.masked_invalid(cmc))

        return df

    def _impute_int_month(self, df):
        if 'int_month' in df:
            return df

        # TODO: this is consistent with the custom code, but should be fixed
        delta = self.config['year_end'] - self.config['year_start']
        if delta == 0:
            int_month = 6
        elif delta == 1:
            int_month = 1
        else:
            int_month = float('NaN')
        df['int_month'] = int_month

        return df

    def _generate_interview_date_cmc(self, df):
        """
        Generates interview date in Century Month Code (CMC) format.
        """
        if not set(['int_year', 'int_month']).issubset(df.columns):
            self.logger.info("Skipping generate interview_date_cmc, int_year "
                             "and int_month not in dataframe columns.")
            df['interview_date_cmc'] = float('NaN')
            return df

        # int_year and int_month supplied by age_calculator in demographics
        # custom code, if input values are supplied
        cmc = [encode_century_month_code(year, month)
               for year, month in
               df[['int_year', 'int_month']].itertuples(index=False)]

        set_default_values('interview_date_cmc', cmc, df, mask=np.ma.masked_invalid(cmc))

        return df

    def _calculate_age_of_death_units_and_number(self, df):
        """
        Populates custom code indicator columns.

        These columns are generated in _generate_continuous_variables but no
        other validation/cleaning is performed. Here is where that occurs.

        The columns are:
            child_age_at_death_raw
            age_of_death_number
            age_of_death_units
        """

        """
        // Create child age at death in months from 3 digit child_age_at_death_raw
                tostring child_age_at_death_raw, replace
                tostring age_of_death_units, replace
                tostring age_of_death_number, replace
                replace age_of_death_units = "" if age_of_death_units == "."
                replace age_of_death_number= "" if age_of_death_number == "."
                replace child_age_at_death_raw = "" if child_age_at_death_raw == "."
                replace age_of_death_units = substr(child_age_at_death_raw, 1, 1) if age_of_death_units =="" & child_age_at_death_raw !=""
                replace age_of_death_number = substr(child_age_at_death_raw, 2, 3) if age_of_death_number =="" & child_age_at_death_raw !=""
                destring age_of_death_units, replace
                destring age_of_death_number, replace


        """  # noqa

        # coerce child_age_at_death_raw to string
        # coerce age_of_death_units to string
        # coerce age_of_death_number to string
        # AND replace all '' values with 'nan' because 'nan' converts to float
        nan = 'nan'  # NOTE: 'nan' and not float('NaN')
        raw = df.child_age_at_death_raw.astype(str).replace('', nan)
        units = df.age_of_death_units.astype(str).replace('', nan)
        number = df.age_of_death_number.astype(str).replace('', nan)

        has_raw = raw != nan

        # there's an implicit record in the raw field. extract character 1 to
        # units and characters 2 and 3 to number
        units[(units == nan) & has_raw] = raw.str[0]
        number[(number == nan) & has_raw] = raw.str[1:4]

        # coerce age_at_death_units and age_at_death_number back to numeric
        df['age_of_death_units'] = units.astype('float64')
        df['age_of_death_number'] = number.astype('float64')

        """
        // If age at death is recorded as time_lived_years but no months and days - convert this to age at death number and units
        if !mi("$time_lived_years") &  mi("$time_lived_months") & mi("$time_lived_days") {
                replace age_of_death_units = 3
                replace age_of_death_number = time_lived_years
                }
        """  # noqa

        if self.config['time_lived_years'] is not None and \
                self.config['time_lived_months'] is None and \
                self.config['time_lived_days'] is None:
            df.age_of_death_units = 3
            df.age_of_death_number = df.time_lived_years

        """
        // If death year is provided but no death month then insert this into age at death units and number and calculate
        if !mi("$death_year") & mi("$death_month") {
                replace age_of_death_units = 3 if age_of_death_units == .
                replace age_of_death_number = death_year-birth_year if age_of_death_number ==.
                }
        """  # noqa

        if self.config['death_year'] and not self.config['death_month']:
            df.loc[df.age_of_death_units.isna(), 'age_of_death_units'] = 3
            df.loc[df.age_of_death_number.isna(), 'age_of_death_number'] = \
                df.death_year - df.birth_year

        return df

    def _ensure_line_id_exists(self, df):
        if 'line_id' not in df:
            df['line_id'] = range(1, len(df) + 1)
        return df

    def _mark_caadm_if_alive(self, df):
        "Update age at death to 6000 IFF child is alive."
        if 'child_alive' in df:
            # replace child_alive_at_death_months = 6000 IFF child_alive == 1
            df.loc[df.child_alive == 'Yes', 'child_age_at_death_months'] = 6000
        return df

    # TODO:  move to bottom
    def _drop_temporary_variables(self, df):
        droppable_columns = ['dod_other_format', 'time_lived_years',
                             'time_lived_months', 'dod', 'death_year',
                             'death_month', 'int_month', 'int_year',
                             'time_lived_days']
        columns_to_drop = [X for X in droppable_columns if X in df]
        return df.drop(columns_to_drop, axis='columns')

    # TODO: this is copy/pasted from age_calculator. REFACTOR
    def _configured(self, *inputs):
        "Predicate: are all input values configured?"
        return all(map(self.config.get, inputs))

    def _not_configured(self, *inputs):
        return not any(map(self.config.get, inputs))

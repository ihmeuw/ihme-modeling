from boltons.iterutils import partition

import pandas
import numpy as np

from winnower.util.categorical import (
    to_Categorical,
)
from winnower.util.dates import encode_century_month_code

from .base import TransformBase

from winnower.globals import eg  # Extraction context globals.

# For module level logging.
# logger = logging.getLogger(__loader__.name)

class Transform(TransformBase):
    one_off_hacked_nids = frozenset([
    ])

    def output_columns(self, input_columns):
        """
        Append extra_columns to input_columns and return it.
        """
        res = input_columns
        extra_columns = []
        if self.config['age_of_death_units']:
            extra_columns.append('age_of_death_units')
        if self.config['child_dod_format']:
            extra_columns.append('child_dod_format')

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
        self._error_if_custom_code_has_unimplemented_nid_specific_code()

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
        # the following section was modified by <USERNAME> - population and
        # fertiltiy needs all women, not just women with children so I'm
        # adding an if statement that subsets to only women with children
        # if you're not me. I only added the if statement -- the "drop
        # if child_alive == ." was original.
        #
        # population/fertility hack (`drop if child_alive == .`)
        #   TODO: how do we support this toggle?
        df = self._keep_only_known_child_outcomes(df)
        if eg.more_debug:
            self.logger.debug("After keep only known child, "
                              f"shape: {df.shape}")

        # Original cbh.do comment:
        # Drop records for still born children
        #
        # Generate born_alive and drop records for still born children
        df = self._generate_born_alive(df)
        if eg.more_debug:
            self.logger.debug(f"After generate born alive, shape: {df.shape}")

        # Original cbh.do comment:
        # Had to shorten var names due to missing vals now rename
        #
        # Rename child_aod_months to child_age_at_death_months (+2 more)
        df.rename(columns={'child_aod_months': 'child_age_at_death_months',
                           # TODO: aod should be age_of_death, but here it was
                           # "at" death
                           'child_aod_raw': 'child_age_at_death_raw',
                           'aod_number': 'age_of_death_number'
                           }, inplace=True)

        # Original cbh.do comment:
        # For categorical variables
        # Age at death units:
        #
        # Calculate age_of_death_units
        df = self._calculate_age_of_death_units(df)

        # Original cbh.do comment:
        # For string variabels which you want to be the meta
        #
        # Calculate child_dod_format (TODO: why is this not an indicator?)
        #   process child_dod_format
        df = self._process_child_dod_format(df)
        if eg.more_debug:
            self.logger.debug("After process child_dod_format, "
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
        #
        # generate interview_date_cmc
        df = self._generate_interview_date_cmc(df)

        # Original cbh.do comment:
        # Generates child_dob_cmc
        #
        # Survey specific
        if self.config["nid"] in [294041, 313076, 350836]:
            """
            // Generates child_dob_cmc
            if "$nid" == "294041" | "$nid" == "313076" | "$nid" == "350836"{
                    gen child_dob_cmc = interview_date_cmc - age_month
                    }
            """
            df["child_dob_cmc"] = df.interview_date_cmc - df.age_month

        # //////////////////////////////////////////////////
        # /// Survey specific modifications/calculations ///
        # //////////////////////////////////////////////////

        # Original cbh.do comment:
        # Adjust dates in IRQ IMIRA 2004
        #
        # Survey specific
        if self.config["nid"] == 23565:
            """
            if "$nid" == "23565" {
            replace time_lived_years = . if time_lived_years > 100
            }
            """
            if "time_lived_years" in df.columns:
                df.loc[df.time_lived_years > 100,
                       "time_lived_years"] = float('NaN')

        # Original cbh.do comment:
        # Adjust death year if in 2 digit format
        #
        # Survey specific
        if self.config["nid"] == 4905:
            """
            if "$nid" == "4905" {
                replace death_year  = death_year +1900
            }
            """
            if "death_year" in df.columns:
                df.death_year == 1900

        # Original cbh.do comment:
        # Adjust <1 month age of death encoding in IND HDS
        # Adjust birth year to YYYY from Y or YY
        #
        # Survey specific
        if self.config["nid"] == 165498:
            """
             if "$nid" == "165498" {
                     replace time_lived_years = 0 if time_lived_years == 55
                     replace time_lived_years = . if time_lived_years == 88
                     replace time_lived_months = 0 if time_lived_months == 55
                     replace time_lived_months = . if time_lived_months == 88
                     replace birth_year = birth_year + 2000 if birth_year < 50
                     replace birth_year = birth_year + 1900 if birth_year > 50
             }
            """

            if "time_lived_years" in df.columns:
                df.loc[df.time_lived_years == 55,
                       "time_lived_years"] = 0
                df.loc[df.time_lived_years == 88,
                       "time_lived_years"] = float('NaN')
            if "time_lived_months" in df.columns:
                df.loc[df.time_lived_months == 55,
                       "time_lived_months"] = 0
                df.loc[df.time_lived_months == 88,
                       "time_lived_months"] = float('NaN')
            if "birth_year" in df.columns:
                df.loc[df.birth_year < 50, "birth_year"] += 2000
                df.loc[df.birth_year > 50, "birth_year"] += 1900

        # Original cbh.do comment:
        # Adjust dates which are not out by full years
        # (remove if this is added to ubcov)
        #
        # Survey specific
        if (self.config["nid"] in
                (20437, 39999, 21240, 20462, 157018, 162317, 286782)):

            """
            if "$nid" == "20437" | "$nid" == "39999" | "$nid" == "21240" | "$nid" == "20462" {
                replace child_dob_cmc = child_dob_cmc+3
                replace interview_date_cmc = interview_date_cmc+3
            }
            """  # noqa
            if "child_dob_cmc" in df.columns:
                df.child_dob_cmc += 3
            if "interview_date_cmc" in df.columns:
                df.interview_date_cmc += 3

        if self.config["nid"] in (19571, 19557, 218568):
            """
            if "$nid" == "19571" | "$nid" == "19557" {
                replace child_dob_cmc = child_dob_cmc-4
                replace interview_date_cmc = interview_date_cmc-4
            }
            """
            if "child_dob_cmc" in df.columns:
                df.child_dob_cmc -= 4
            if "interview_date_cmc" in df.columns:
                df.interview_date_cmc -= 4

        # This fails because winnower drops (or doesn't create) the
        # child_dob_cmc column. Ubcov does create it but it is empty.
        #
        # Checking for the existence of the child_dob_cmc column before
        # doing any calculations on it results in winnower having the
        # same result as ubcov ... in other words this nid custom code
        # for 20450 is basically a no-op.
        #
        # Survey specific
        if self.config["nid"] == 20450:
            """
            if "$nid" == "20450" {
                replace child_dob_cmc = child_dob_cmc+2
                replace interview_date_cmc = interview_date_cmc+2
            }
            """
            if "child_dob_cmc" in df.columns:
                df.child_dob_cmc += 2
            if "interview_date_cmc" in df.columns:
                df.interview_date_cmc += 2

        if self.config["nid"] == 9278:
            """
            if "$nid" == "9278" {
                    replace mother_birth_month = 0 if mother_birth_month == 98
                    }
            """
            df.loc[df.mother_birth_month == 98, "mother_birth_month"] = 0

        if self.config["nid"] in [9278, 9270, 27630]:
            """
            if "$nid" == "27630"{
                    replace child_dob_cmc = interview_date_cmc - age_month
                    }
            if "$nid" == "9270"{
                    replace child_dob_cmc = interview_date_cmc - age_month
                    }
            if "$nid" == "9278"{
                    replace child_dob_cmc = interview_date_cmc - age_month
                    }
            """
            df.child_dob_cmc = df.interview_date_cmc - df.age_month

        if self.config["nid"] in (27582, 27590):
            """
            if "$nid" == "27582"{
                replace child_dob_cmc = interview_date_cmc - age_month
            }

            if "$nid" == "27590"{
                replace child_dob_cmc = interview_date_cmc - age_month
            }
            """

            if set(["interview_date_cmc", "age_month"]).issubset(df.columns):
                df["child_dob_cmc"] = df.interview_date_cmc - df.age_month

        if self.config["nid"] == 27621:
            """
            if "$nid" == "27621"{
                replace child_dob_cmc = interview_date_cmc - age_month
                replace mother_birth_month = . if mother_birth_month == 98
                replace mother_birth_year = . if mother_birth_year == 9898
            }
            """
            if set(["interview_date_cmc", "age_month"]).issubset(df.columns):
                df["child_dob_cmc"] = df.interview_date_cmc - df.age_month
            if "mother_birth_month" in df.columns:
                df.loc[df.mother_birth_month == 98,
                       "mother_birth_month"] = float("NaN")
            if "mother_birth_year" in df.columns:
                df.loc[df.mother_birth_year == 9898,
                       "mother_birth_year"] = float("NaN")

        if self.config["nid"] in (13218, 27511):
            """
            if "$nid" == "13218"{
                replace mother_dob_cmc = mother_dob_cmc - 23000
            }
            """
            if "mother_dob_cmc" in df.columns:
                df.mother_dob_cmc -= 23000

        if self.config["nid"] == 5583:
            """
            if "$nid" == "5583"{
                drop if mother_birth_month != 3
                drop if mother_age_years < 15 | mother_age_years > 49
                drop if pweight == 0
                drop mother_birth_month
                replace mother_age_months = 6 if mother_age_months > 12
            }
            """
            pass

        if self.config["nid"] == 46480:
            """
            if "$nid" == "46480"{
                replace interview_date_cmc = interview_date_cmc + 520
                replace child_dob_cmc = child_dob_cmc + 520
            }
            """
            if "interview_date_cmc" in df.columns:
                df.interview_date_cmc += 520

            if "child_dob_cmc" in df.columns:
                df.child_dob_cmc += 520

        if self.config["nid"] == 27615:
            """
            if "$nid" == "27615"{
                replace mother_birth_month = 0 if mother_birth_month == 98
                drop if mother_birth_year == 9898
            }
            """

            if "mother_birth_month" in df.columns:
                df.loc[df.mother_birth_month == 98,
                       "mother_birth_month"] = 0
            if "mother_birth_year" in df.columns:
                df.loc[df.mother_birth_year == 9898,
                       "mother_birth_year"] = float("NaN")

        # /////////////////////////////////////////////////////////////////////
        # ///Calculating age at death months from various available variables//
        # /////////////////////////////////////////////////////////////////////

        # Original cbh.do comment:
        # Create child age at death in months from 3 digit
        # child_age_at_death_raw.
        #
        # If age at death is recorded as time_lived_years but no months
        # and days - convert this to age at death number and units
        # If death year is provided but no death month then insert this
        # into age at death units and number and calculate
        #
        # age_at_death_units + age_at_death_number
        df = self._calculate_age_of_death_units_and_number(df)

        # Original cbh.do comment:
        # Create age at death from units and number variables
        # Create age at death from date of death
        # Create age at death months if given as days AND months AND years
        # Add any more formats  to change for age at death in here
        #
        # Calculate child_age_at_death_months
        df = self._calculate_age_at_death_months(df)

        # Original cbh.do comment:
        # --NONE--
        #
        # clean mother_birth_year
        df = self._clean_mother_birth_year(df)

        # Original cbh.do comment:
        # --NONE--
        #
        # mother_dob_cmc modifications
        df = self._generate_mother_dob_cmc(df)

        # Original cbh.do comment:
        # --NONE--
        #
        # sex_hhm_file_male

        # Original cbh.do comment:
        # --NONE--
        #
        # generate birthtointerview_cmc
        if set(["child_dob_cmc", "interview_date_cmc"]).issubset(df.columns):
            df['birthtointerview_cmc'] =\
                df.interview_date_cmc - df.child_dob_cmc

        # ////////////////////////////////////////////////////
        # /// Any model specific requirements for variables///
        # ////////////////////////////////////////////////////

        # Original cbh.do comment:
        # Recode child_age_at_death_months to 6000 for any children alive
        #
        # Mark living children as having a non-NaN child_age_at_death_months
        df = self._mark_caadm_if_alive(df)

        # Original cbh.do comment:
        # Rename variables for use in U5M model
        #
        renames = {'sex_id': 'child_sex'}
        if 'child_id' not in df:
            renames['line_id'] = 'child_id'
        df.rename(columns=renames, inplace=True)

        # Survey specific
        if self.config["nid"] in (2531, 2347, 2577):
            """
            if "$nid" == "2531" {
                replace child_alive = 1 if death_year == . & child_dob_cmc != .
                replace child_alive = 0 if death_year != . & child_dob_cmc != .
                replace child_alive = . if death_year == . & child_dob_cmc == .
                label values child_alive YN
                replace child_age_at_death_months = cmc_death_date - child_dob_cmc if child_alive == 0 & death_year != .
                replace child_age_at_death_months = 6000 if child_alive == 1
            }

            if "$nid" == "2347" {
                    replace child_alive = 1 if death_year == . & child_dob_cmc != .
                    replace child_alive = 0 if death_year != . & child_dob_cmc != .
                    replace child_alive = . if death_year == . & child_dob_cmc == .
                    label values child_alive YN
                    replace child_age_at_death_months = cmc_death_date - child_dob_cmc if child_alive == 0 & death_year != .
                    replace child_age_at_death_months = 6000 if child_alive == 1
            }

            if "$nid" == "2577" {
                    replace child_alive = 1 if death_year == . & child_dob_cmc != .
                    replace child_alive = 0 if death_year != . & child_dob_cmc != .
                    replace child_alive = . if death_year == . & child_dob_cmc == .
                    label values child_alive YN
                    replace child_age_at_death_months = cmc_death_date - child_dob_cmc if child_alive == 0 & death_year != .
                    replace child_age_at_death_months = 6000 if child_alive == 1
            }
            """  # noqa

            if set(["child_alive",
                    "death_year",
                    "child_dob_cmc"]).issubset(df.columns):

                df.loc[df.death_year.isna() & df.child_dob_cmc.notna(),
                       "child_alive"] = 'Yes'
                df.loc[df.death_year.notna() & df.child_dob_cmc.notna(),
                       "child_alive"] = 'No'
                df.loc[df.death_year.isna() & df.child_dob_cmc.isna(),
                       "child_alive"] = np.nan

                if set(["child_age_at_death_months",
                        "cmc_death_date"]).issubset(df.columns):

                    df.loc[(df.child_alive == 'No') & df.death_year.notna(),
                           "child_age_at_death_months"] =\
                        df.cmc_death_date - df.child_dob_cmc

                if "child_age_at_death_months" in df.columns:
                    df.loc[df.child_alive == 'Yes',
                           "child_age_at_death_months"] = 6000

        if self.config['nid'] == 105306:
            """
            drop child_sex
            merge 1:1 hh_id mother_id child_id using "<FILEPATH>"
            label define child_sex 1 "Male" 2 "Female"
            label values child_sex child_sex
            gen mother_birth_month = 6
            drop mother_dob_cmc
            //gen mother_birth_year = int_year - $mother_age_years
            gen mother_dob_cmc = 12 * (mother_birth_year - 1900) + mother_birth_month
            drop if mother_age_years < 15 | mother_age_years > 49
            """
            df.drop('child_sex', axis=1, inplace=True)
            from winnower.sources.stata import pyreadstat_load_stata
            right_df = pyreadstat_load_stata("<FILEPATH>")
            from winnower.transform import Merge
            merge_cols = ['hh_id', 'mother_id', 'child_id']
            merge_transform = Merge.from_dataframe(right_df,
                                                   master_cols=merge_cols,
                                                   using_cols=merge_cols,
                                                   merge_type="1:1")
            df = merge_transform.execute(df)
            df['child_sex'] = to_Categorical(df.child_sex,
                                             {1: 'Male', 2: 'Female'})

            df['mother_birth_month'] = 6
            if 'mother_dob_cmc' in df: # no need for line below
                df.drop('mother_dob_cmc', axis=1, inplace=True)
            # mother_birth_year was not configured, added yr_birth
            df['mother_dob_cmc'] = 12 * (df.mother_birth_year - 1900) \
                + df.mother_birth_month

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

    def _error_if_custom_code_has_unimplemented_nid_specific_code(self):
        nid = self.config['nid']
        if nid in self.one_off_hacked_nids:
            msg = f"Hack exists for NID {nid} - cannot extract yet"
            raise NotImplementedError(msg)

    def _generate_continuous_variables(self, df):
        # here we generate several columns in a manner that appears to mirror
        # regular Continuous ("cont") indicator generation.
        columns = ['child_aod_months', 'child_aod_raw', 'aod_number',
                   'death_month', 'death_year', 'child_dod', 'time_lived_days',
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

    def _keep_only_known_child_outcomes(self, df):
        if 'child_alive' in df:
            # TODO: must support some faux toggle by user to skip this...
            # drop any rows where child_alive is N/A
            df.dropna(subset=['child_alive'], inplace=True)
        return df

    def _generate_born_alive(self, df):
        """
         Drop records for still born children
         if !mi("$born_alive"){ //& "`c(username)'" != "bstrub" & "`c(username)'" != "gmanny" & "`c(username)'" != "narian"{
             gen born_alive = 0
             replace born_alive = 1 if inlist($born_alive, $born_alive_yes)
             replace born_alive = . if $born_alive == .
             drop if born_alive == 0
             noisily display as error "born_alive"
             print_rows
         }

        """  # noqa

        ba_config = self.config['born_alive']
        if pandas.isnull(ba_config):
            return df

        _born_alive = self.config['born_alive']
        _born_alive_yes = self.config['born_alive_yes']
        ind_config = {'born_alive': _born_alive,
                      'born_alive_true': _born_alive_yes,
                      # no false values provided; behave as if there are none
                      'born_alive_false': '',
                      'ubcov_id': self.config['ubcov_id']}

        transform = self._manual_indicator('born_alive', 'bin', ind_config)
        transform.validate(input_columns=df.columns)

        df = transform.execute(df)

        # Finally, drop any observations where child was not born alive and
        # born alive is unknown.
        if _born_alive in df.columns:
            df.drop(df.index[(df.born_alive != 1) & df[_born_alive].notna()],
                    inplace=True)

        return df

    def _manual_indicator(self, ind_name, ind_type, ind_config):
        from winnower.config.models.setup_forms import IndicatorForm
        from winnower.config.models.reflect import get_form
        from winnower.transform.indicators import transform_from_indicator
        ind = IndicatorForm(indicator_name=ind_name,
                            topic_name='cbh',
                            indicator_type=ind_type)
        form = get_form([ind], all_fields=ind_config.keys())
        config = form(**ind_config)
        transform = transform_from_indicator(ind, config, label_helper=None)
        return transform

    def _calculate_age_of_death_units(self, df):
        units = pandas.Series(float('NaN'), index=df.index)

        col_raw = self.config['age_of_death_units']
        if col_raw is not None:
            col = self.get_column_name(col_raw)
            raw_units = df[col]
            from winnower.util.dataframe import as_column_type
            # get days/months/years/weeks vals; coerce to dtype of raw_units,
            # and then assign mapped values 1-4 to units (respectively)
            unit_values = [self.config[X] for X in ('aod_units_days_vals',
                                                    'aod_units_months_vals',
                                                    'aod_units_years_vals')]
            week_value = self.config['aod_units_weeks_vals']
            if week_value is not None:
                unit_values.append(week_value)
            converted_values = as_column_type(raw_units, unit_values)

            for unit_value, raw_value in enumerate(converted_values, start=1):
                units[raw_units == raw_value] = unit_value

        df['age_of_death_units'] = units
        return df

    def _process_child_dod_format(self, df):
        child_dod_format = self.config['child_dod_format']
        if child_dod_format:
            """
            if !mi("$child_dod_format") {
                gen child_dod_format = "$child_dod_format"
            }
            """
            df['child_dod_format'] = child_dod_format
        else:
            """
            else {
                gen child_dod_format = .
            }
            """
            df['child_dod_format'] = np.nan
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
        if !mi("$child_dod") & "$child_dod_format" == "dd/mm/yyyy" {
            split $child_dod, parse("/") gen(temp)
            destring temp2, replace
            destring temp3, replace
            replace temp2 = . if temp2 >12
            replace temp3 = . if temp3 >3000
            replace cmc_death_date = 12*(temp3-1900)+temp2
            replace child_age_at_death_months = cmc_death_date - child_dob_cmc
        }
        """
        if self.config['child_dod'] and \
                self.config['child_dod_format'] == 'dd/mm/yyyy':
            temp = df['child_dod'].str.split('/', expand=True)
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
        self.logger.debug("generating child_dob_cmc")
        df['child_dob_cmc'] = cmc
        return df

    def _clean_mother_birth_year(self, df):
        """
        Cleans the mothers birth year if it exists.

        """

        """
        if !mi("$mother_birth_year"){
                replace mother_birth_year = mother_birth_year + 1900 if mother_birth_year < 100
        }
        """  # noqa

        if "mother_birth_year" in df.columns:
            df.loc[df.mother_birth_year < 100,
                   "mother_birth_year"] += 1900

        return df

    def _generate_mother_dob_cmc(self, df):
        """
        Generates the mother's data of birth (dob) in CMC format if possible.

        If mother_birth_year and mother_birth_month exist then they are
        used to calculate dob even if mother_dob_cmc exists in the data.

        The original R cbh.do file also generates an empty mother_dob_cmc
        column if it doesn't exist in the orignal data. I am including that
        code but commenting out for now until a case that require it appears.
        """

        """
        Generate missing mother_dob_cmc if it doesn't exist.

        if mi("$mother_dob_cmc"){
                gen mother_dob_cmc = .
        }
        """

        mother_dob_cmc = "mother_dob_cmc"
        mother_birth_year = "mother_birth_year"
        mother_birth_month = "mother_birth_month"

        # if mother_dob_cmc not in df.columns:
        #     # It doesn't exist so create it and fill with NaN.
        #     df[mother_dob_cmc] = float('NaN')

        """
        Calculate mother_dob_cmc from mother_birth_year and mother_birth_month
        if present.

        if !mi("$mother_birth_year") & !mi("$mother_birth_month"){
                replace mother_dob_cmc = 12*(mother_birth_year - 1900) + mother_birth_month
                //drop mother_birth_year mother_birth_month
        }
        """  # noqa

        if (mother_birth_year in df.columns
                and mother_birth_month in df.columns):

            cmc = [encode_century_month_code(year, month)
                   for year, month in
                   df[[mother_birth_year,
                       mother_birth_month]].itertuples(index=False)]

            df[mother_dob_cmc] = cmc

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
        df['interview_date_cmc'] = cmc
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
        if self.config['mother_birth_month'] is None:
            return df

        droppable_columns = ['child_dod_format', 'time_lived_years',
                             'time_lived_months', 'child_dod', 'death_year',
                             'death_month', 'int_month', 'int_year',
                             'mother_birth_month', 'mother_birth_year',
                             'time_lived_days']
        columns_to_drop = [X for X in droppable_columns if X in df]
        return df.drop(columns_to_drop, axis='columns')

    # TODO: this is copy/pasted from age_calculator. REFACTOR
    def _configured(self, *inputs):
        "Predicate: are all input values configured?"
        return all(map(self.config.get, inputs))

    def _not_configured(self, *inputs):
        return not any(map(self.config.get, inputs))

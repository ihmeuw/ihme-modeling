"""
Custom code for Risky Sexual Practices.
"""
import numpy
import pandas

from winnower.errors import Error
from winnower.util.categorical import to_Categorical
from winnower.util.dataframe import set_default_values

from .base import TransformBase


# Should this be moved to a DB? Then the logic would get more complex...
MaritalStatusCategories = ('stand-in', 'currently married',
                           'living with partner', 'widowed', 'divorced',
                           'separated', 'never married', 'other')


def logical_and(*args):
    """
    Returns the logical AND of the arguments.

    All arguments should be pandas DataFrames.
    """
    if len(args) < 2:
        n = len(args)
        raise Error(f"Must provide 2+ args to logical_and, not {n}")
    # smarter than reduce(numpy.logical_and, args)
    # https://stackoverflow.com/a/20528566
    return numpy.all(args, axis=0)


class Transform(TransformBase):
    """
    Replicates logic of rsp.do
    """
    def output_columns(self, input_columns):
        try:  # rsp custom code removes 'birth_year'
            input_columns.remove('birth_year')
        except ValueError:  # not present
            pass

        # In special cases this will generate num_partners_year
        mod = self.config['survey_module']
        nid = self.config['nid']
        if any([self.config['survey_name'] == 'UNICEF_MICS',
                nid == 20322 and mod == "MN",
                nid == 19579 and mod == "MN",
                nid == 27952]) and 'num_partners_year' not in input_columns:
            input_columns.append('num_partners_year')

        return input_columns + [
            'marital_status',
            'client_sex_worker_2_partner',
            'condom_last_time',
            'debut_age',
            'had_intercourse',
            'sex_in_last_12_months'
        ]

    def execute(self, df):
        df = self._adjust_surveys(df)
        df = self._calc_marital_status(df)
        df = self._calc_partner_away(df)
        df = self._calc_had_intercourse(df)
        df = self._calc_debut_age(df)
        df = self._calc_sex_in_last_12_months(df)
        df = self._calc_condom_last_time(df)
        df = self._calc_num_partners_year(df)
        df = self._calc_condom_every_time(df)
        df = self._calc_client_sex_worker(df)
        df = self._calc_num_partners_lifetime(df)
        df = self._drop_scratch_columns(df)
        return df

    def _adjust_surveys(self, df):
        """
        SURVEY ADJUSTMENTS section
        """
        if self.config['nid'] == 4694 and self.config['survey_module'] == 'MN':
            no_pweight = numpy.where(df.pweight.isna())[0]
            return df.drop(no_pweight, axis='rows')
        elif self.config['nid'] == 27987:
            bad_psu_mask = (df.psu > 105) | (df.psu.isna())
            bad_psu = numpy.where(bad_psu_mask)[0]
            return df.drop(bad_psu, axis='rows')
        return df

    def _calc_marital_status(self, df):
        """
        Generate categorical marital_status from input binary indicators.
        """
        # compute series per stata code
        ms = pandas.Series(float('NaN'), index=df.index, name='marital_status')
        # these may potentialy overwrite one another, so last == most important
        if 'marital_status_other' in df:
            ms[df.marital_status_other == 1] = 7
        if 'never_married' in df:
            ms[df.never_married == 1] = 6
        if 'separated' in df:
            ms[df.separated == 1] = 5
        if 'divorced' in df:
            ms[df.divorced == 1] = 4
        if 'widowed' in df:
            ms[df.widowed == 1] = 3
        if 'currently_married' in df:
            ms[df.currently_married == 1] = 1
        if 'living_with_partner' in df:
            ms[df.living_with_partner == 1] = 2

        df['marital_status'] = to_Categorical(ms, MaritalStatusCategories)
        return df

    def _calc_partner_away(self, df):
        set_default_values('partner_away', numpy.nan, df)
        if self.config['nid'] in {22112, 22114}:
            # nid 22114 did not ask partner away if respondant says status is
            # living with partner'
            df.loc[df.marital_status == 'living with partner', 'partner_away'] = 0  # noqa
        return df

    def _calc_had_intercourse(self, df):
        # NID 22116 (BWA AIDS impact 2008) only asks had intercourse if
        # respondent has not had a child so had to combine both vars to get had
        # intercourse. Have to correct that combination so that people who have
        # not had a child but are missing a response to had intercourse are
        # still marked as missing had intercourse
        if self.config['nid'] == 22116:
            q301a_chld = self.get_column_name('q301a_chld')
            q301b_ever = self.get_column_name('q301b_ever')
            mask = logical_and(df[q301a_chld] == 2, df[q301b_ever].isna())
            df.loc[mask, 'had_intercourse'] = numpy.nan
        elif self.config['nid'] == 27031:
            #  NID 27031 (MOZ MICS WN 2008-09) only asks had intercourse if the
            # respondent has never been married/lived with a man so have to
            # combine both vars to get had intercourse.
            df['had_intercourse'] = numpy.nan
            ma8a = self.get_column_name('ma8a')
            ma1 = self.get_column_name('ma1')
            ma3 = self.get_column_name('ma3')
            df.loc[df[ma8a] == 2, 'had_intercourse'] = 0
            mask = df[ma1].isin({1, 2}) | df[ma3].isin({1, 2}) | (df[ma8a] == 1)  # noqa
            df.loc[mask, 'had_intercourse'] = 1

        return df

    def _calc_debut_age(self, df):
        set_default_values('debut_age', numpy.nan, df)
        set_default_values('had_intercourse', numpy.nan, df)
        set_default_values('age_at_first_union', numpy.nan, df)
        set_default_values('age_at_first_union_year', numpy.nan, df)
        set_default_values('birth_year', numpy.nan, df)

        if self.config['nid'] == 248224 and \
                self.config['survey_module'] == 'MN':
            #  NID 28224 MN (MLI MICS MN 2015) asks 2 different questions for
            # age at first union depending on if the respondent was married
            # once or multiple times. This adjustment is needed to bring in the
            # responses from men who were married multiple times (and were only
            # asked MMA8BY)
            mma8by = self.get_column_name('mma8by')
            df.loc[df[mma8by] < 9997, 'age_at_first_union_year'] = df[mma8by]

        if self.config['survey_name'] == 'MACRO_KAP':
            # MACRO KAP gives birth year as the full year but age at first
            # union as just the last two digits
            df['age_at_first_union_year'] += 1900

        # impute age at first union
        df['impute'] = df.age_at_first_union_year - df.birth_year
        df.loc[df['impute'] < 0, 'impute'] = numpy.nan
        df.loc[df['impute'] > 95, 'impute'] = numpy.nan

        min_values = df[['debut_age', 'age_at_first_union', 'impute']].min(
            axis='columns')
        if self.config['survey_name'] in {'MACRO_DHS', 'MACRO_AIS', 'MACRO_KAP'}:  # noqa
            df.loc[df.debut_age == 96, 'debut_age'] = min_values
            df.loc[df.debut_age == 96, 'debut_age'] = float('NaN')  # necessary
        elif self.config['survey_name'] in {"ZMB/SEXUAL_BEHAVIOR_SURVEY", "KEN/AIDS_INDICATOR_SURVEY", "UNICEF_MICS", "NGA/HH_SCHOOL_AND_HEALTH_FACILITY_SURVEYS"}:  # noqa
            df.loc[df.debut_age == 95, 'debut_age'] = min_values
            df.loc[df.debut_age == 95, 'debut_age'] = float('NaN')  # necessary

        df.loc[df.had_intercourse == 0, 'debut_age'] = 0
        return df

    def _calc_sex_in_last_12_months(self, df):
        set_default_values('sex_in_last_12_months', numpy.nan, df)
        if 'last_birth_cmc' in df and self.config['survey_name'] in {'MACRO_DHS', 'MACRO_AIS'}:  # noqa
            # v527 (sex in last 12 months) codes 996 as "before last birth"
            # note we cannot use sex_in_last_12_months becuase it has already
            # been transformed into a Binary column
            is_before_last_birth = df[self.get_column_name('v527')] == 996
            df.loc[is_before_last_birth, 'sex_in_last_12_months'] = 0

            # months since last birth
            mslb = 12 * (df['int_year'] - 1900) + df['int_month'] - df['last_birth_cmc']  # noqa
            birth_in_last_year = mslb < 12
            df.loc[is_before_last_birth & birth_in_last_year, 'sex_in_last_12_months'] = 1  # noqa

        if self.config['survey_name'] == 'MACRO_DHS':
            if self.config['survey_module'] == 'WN':
                v527 = self.get_column_name('v527')
                df.loc[df[v527] == 995, 'sex_in_last_12_months'] = 1
            elif self.config['survey_module'] == 'MN':
                mv527 = self.get_column_name('mv527')
                df.loc[df[mv527] == 995, 'sex_in_last_12_months'] = 1

        # some issues with coding in early DHS that needs correction - see
        # codebooks for full explanation
        if self.config['nid'] == 20852:
            if self.config['survey_module'] == 'WN':
                v527 = self.get_column_name('v527')
                df.loc[df[v527] == 994, 'sex_in_last_12_months'] = 0
            if self.config['survey_module'] == 'MN':
                mv527 = self.get_column_name('mv527')
                df.loc[df[mv527] == 994, 'sex_in_last_12_months'] = 0
        elif self.config['nid'] == 20976:
            if self.config['survey_module'] == 'WN':
                v527 = self.get_column_name('v527')
                df.loc[df[v527] == 994, 'sex_in_last_12_months'] = numpy.nan
            if self.config['survey_module'] == 'MN':
                mv527 = self.get_column_name('mv527')
                df.loc[df[mv527] == 994, 'sex_in_last_12_months'] = numpy.nan
        elif self.config['nid'] == 50393:
            q807d = self.get_column_name('q807d')
            df.loc[df[q807d].notna(), 'sex_in_last_12_months'] = 0

        # asks last sex how many days, weeks, months, years all as separate
        # questions
        if self.config['nid'] == 50393:
            q807d = self.get_column_name('q807d')
            df.loc[df[q807d].notna(), 'sex_in_last_12_months'] = 0

        return df

    def _calc_condom_last_time(self, df):
        set_default_values('condom_last_time', numpy.nan, df)

        nid = self.config['nid']
        if nid == 313076:
            # 313076 skips condom last time question if respondent answers
            # 'never' to how often do you use condom with most recent sexual
            # partner
            df['condom_last_time'] = numpy.nan
            q6_19_1 = self.get_column_name('q6_19_1')
            df.loc[df[q6_19_1] == 1, 'condom_last_time'] = 1
            q6_18_1 = self.get_column_name('q6_18_1')
            mask = numpy.logical_or(df[q6_18_1] == 4, df[q6_19_1] == 2)
            df.loc[mask, 'condom_last_time'] = 0
        elif nid == 12102:
            # nid 12102 skips condom last time if respondent answers no to ever
            # used condom (cq6_1)
            df['condom_last_time'] = numpy.nan
            cq6_4 = self.get_column_name('cq6_4')
            df.loc[df[cq6_4] == 1, 'condom_last_time'] = 1
            cq6_1 = self.get_column_name('cq6_1')
            mask = numpy.logical_or(df[cq6_1] == 2, df[cq6_4] == 2)
            df.loc[mask, 'condom_last_time'] = 0
        elif nid == 228102:
            # nid 228102 skips condom last time if respondent answers no to
            # ever used condom (q7_2)
            df['condom_last_time'] = numpy.nan
            q7_3 = self.get_column_name('q7_3')
            df.loc[df[q7_3] == 1, 'condom_last_time'] = 1
            q7_2 = self.get_column_name('q7_2')
            mask = numpy.logical_or(df[q7_2] == 2, df[q7_3] == 2)
            df.loc[mask, 'condom_last_time'] = 0
        elif nid == 313074:
            # nid 313074 skips condom last time if respondent answers no to
            # ever used condom (q7_1)
            df['condom_last_time'] = numpy.nan
            q7_2 = self.get_column_name('q7_2')
            df.loc[df[q7_2] == 1, 'condom_last_time'] = 1
            q7_1 = self.get_column_name('q7_1')
            mask = numpy.logical_or(df[q7_1] == 2, df[q7_2] == 2)
            df.loc[mask, 'condom_last_time'] = 0

        # for surveys that don't restrict to last 12 months
        if 'sex_in_last_12_months' in df:
            if nid in {19571, 18938, 21139} and self.config['survey_module'] == 'MN':  # noqa
                df.loc[df.sex_in_last_12_months == 0, 'condom_last_time'] = numpy.nan  # noqa
            elif nid in {9076, 19614, 18531, 20537, 20132, 19198, 20909, 19305, 20382, 20212, 20301, 19546, 19370, 228102, 313074, 12102, 313076, 18938}:  # noqa
                df.loc[df.sex_in_last_12_months == 0, 'condom_last_time'] = numpy.nan  # noqa
        return df

    def _calc_num_partners_year(self, df):
        if 'num_partners_spousal_adjust' in df:
            df['num_partners_year'] += df['num_partners_spousal_adjust']

        mod = self.config['survey_module']
        nid = self.config['nid']

        if any([self.config['survey_name'] == 'UNICEF_MICS',
                nid == 20322 and mod == "MN",
                nid == 19579 and mod == "MN",
                nid == 27952]):
            set_default_values('num_partners_year', numpy.nan, df)

            if 'sex_in_last_12_months' in df:
                df.loc[df.sex_in_last_12_months == 0, 'num_partners_year'] = 0

            if 'unicef_1_partner' in df:
                df.loc[df.unicef_1_partner == 0, 'num_partners_year'] = 1
                del df['unicef_1_partner']

            if 'unicef_2_partner' in df:
                df.loc[df.unicef_2_partner == 0, 'num_partners_year'] = 2
                del df['unicef_2_partner']

        if 'num_partners_year' in df:
            df.loc[df.had_intercourse == 0, 'num_partners_year'] = 0
            if 'sex_in_last_12_months' in df:
                mask = logical_and(df.num_partners_year.isna(),
                                   df.sex_in_last_12_months == 0)
                df.loc[mask, 'num_partners_year'] = 0

        if 'num_partners_lifetime' in df:
            df.loc[df.had_intercourse == 0, 'num_partners_lifetime'] = 0

        return df

    def _calc_condom_every_time(self, df):
        # DHS 4-7 and AIS skip condom every time based on condom last time so
        # have to reconstruct skipped negative responses
        if 'condom_every_time_1' in df and 'condom_last_time' in df:
            df.loc[df.condom_last_time == 0, 'condom_every_time_1'] = 0

        if 'condom_every_time_2' in df and 'condom_last_time_2' in df:
            df.loc[df.condom_last_time_2 == 0, 'condom_every_time_2'] = 0
            del df['condom_last_time_2']

        if 'condom_every_time_3' in df and 'condom_last_time_3' in df:
            df.loc[df.condom_last_time_3 == 0, 'condom_every_time_3'] = 0
            del df['condom_last_time_3']

        if 'num_partners_year' not in df:
            self.logger.warning("MISSING NUM PARTNERS YEAR. ALL CONDOM "
                                "EVERY TIME 2 & 3 PARTNER WILL BE MISSING")
            return df

        set_default_values('condom_every_time_2_partner', numpy.nan, df)

        # These long names interfere with PEP8 - we're using abbreviations
        cet1, cet2, cet3 = [f'condom_every_time_{i}' for i in (1, 2, 3)]
        cet1p, cet2p, cet3p = [f'{c}_partner' for c in (cet1, cet2, cet3)]

        one_partner = df.num_partners_year == 1
        if cet1 in df and cet2 in df and cet3 in df:
            set_default_values(cet3p, numpy.nan, df)
            # One partner
            df.loc[one_partner, cet3p] = df.condom_every_time_1
            # Two partners
            two_partner = df.num_partners_year == 2
            two_always_condom = logical_and(df[cet1] == 1, df[cet2] == 1)

            two_known_condom = logical_and(df[cet1].notna(), df[cet2].notna())

            # ~ logically negates (flips True to False)
            df.loc[two_partner & ~two_known_condom, cet3p] = numpy.nan
            df.loc[two_partner & two_known_condom, cet3p] = 0
            df.loc[two_partner & two_always_condom, cet3p] = 1
            # Three or more partners
            tre_partners = numpy.logical_or(df.num_partners_year > 2,
                                            df.num_partners_year.isna())
            tre_always_condom = logical_and(df[cet1] == 1,
                                            df[cet2] == 1,
                                            df[cet3] == 1)
            tre_known_condom = logical_and(df[cet1].notna(),
                                           df[cet2].notna(),
                                           df[cet3].notna())
            df.loc[tre_partners & ~tre_known_condom, cet3p] = numpy.nan
            df.loc[tre_partners & tre_known_condom, cet3p] = 0
            df.loc[tre_partners & tre_always_condom, cet3p] = 1

        if cet1 in df and cet2 in df:
            df.loc[one_partner, cet2p] = df.condom_every_time_1

            multi_partner = numpy.logical_or(df.num_partners_year > 1,
                                             df.num_partners_year.isna())
            always_condom = logical_and(df.condom_every_time_1 == 1,
                                        df.condom_every_time_2 == 1)
            known_condom = logical_and(df[cet1].notna(), df[cet2].notna())
            df.loc[multi_partner & ~known_condom, cet2p] = numpy.nan
            df.loc[multi_partner & known_condom, cet2p] = 0
            df.loc[multi_partner & always_condom, cet2p] = 1

        return df

    def _calc_client_sex_worker(self, df):
        if 'num_partners_year' not in df:
            self.logger.warning("MISSING NUM PARTNERS YEAR. ALL CLIENT SEX "
                                "WORKER 2 & 3 PARTNER WILL BE MISSING")
            return df

        # use abbreviations for PEP8
        csw1, csw2, csw3 = [f'client_sex_worker_{i}' for i in (1, 2, 3)]
        csw1p, csw2p, csw3p = [f'{csw}_partner' for csw in (csw1, csw2, csw3)]
        if csw1 not in df:
            return df

        set_default_values(csw2p, numpy.nan, df)

        # One partner
        one_partner = df.num_partners_year == 1
        df.loc[one_partner, csw2p] = df[csw1]
        # Two or more partners
        multi_partner = numpy.logical_or(df.num_partners_year > 1,
                                         df.num_partners_year.isna())
        known_worker = logical_and(df[csw1].notna(), df[csw2].notna())
        not_worker = logical_and(df[csw1] == 0, df[csw2] == 0)

        df.loc[logical_and(multi_partner, ~known_worker), csw2p] = numpy.nan
        df.loc[logical_and(multi_partner, known_worker), csw2p] = 1
        df.loc[logical_and(multi_partner, not_worker), csw2p] = 0

        if csw3 not in df:
            return df

        set_default_values(csw3p, numpy.nan, df)

        # One partner
        df.loc[one_partner, csw3p] = df[csw1]
        # Two partners
        two_partner = df.num_partners_year == 2
        df.loc[logical_and(two_partner, ~known_worker), csw3p] = numpy.nan
        df.loc[logical_and(two_partner, known_worker), csw3p] = 1
        df.loc[logical_and(two_partner, not_worker), csw3p] = 0
        # Three or more partners
        tre_partner = numpy.logical_or(df.num_partners_year > 2,
                                       df.num_partners_year.isna())
        df.loc[logical_and(tre_partner, ~known_worker, ~df[csw3].notna()),
               csw3p] = numpy.nan
        df.loc[logical_and(tre_partner, known_worker, df[csw3].notna()),
               csw3p] = 1
        df.loc[logical_and(tre_partner, not_worker, df[csw3] == 0),
               csw3p] = 0
        return df

    def _calc_num_partners_lifetime(self, df):
        if self.config['nid'] == 20167 and self.config['survey_module'] == 'WN':  # noqa
            df.loc[df.had_intercourse == 0, 'num_partners_lifetime'] = 0
            df.loc[df.unicef_1_partner == 0, 'num_partners_lifetime'] = 1
            del df['unicef_1_partner']
        return df

    def _drop_scratch_columns(self, df):
        # Always created / always need dropping
        to_drop = [
            'age_at_first_union',
            'age_at_first_union_year',
            'birth_year',
            'impute']

        # scratch columns which may need to be cleaned
        conditional_scratch = (
            # marital status
            'currently_married',
            'never_married',
            'widowed',
            'divorced',
            'separated',
            'living_with_partner',
            'marital_status_other',
            # sex in last 12 months
            'months_since_last_birth',
            # num partners year
            'num_partners_spousal_adjust',
            # condom every time
            'condom_every_time_1',
            'condom_every_time_2',
            'condom_every_time_3',
            # client sex worker
            'client_sex_worker_1',
            'client_sex_worker_2',
            'client_sex_worker_3',
            # num partners lifetime
            'unicef_1_partner',
        )

        to_drop.extend(X for X in conditional_scratch if X in df)
        return df.drop(to_drop, axis='columns')

import pandas as pd
from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders import get_current_cause_hierarchy
from cod_prep.downloaders import add_code_metadata
from cod_prep.downloaders import get_cause_map
from cod_prep.downloaders import add_age_metadata
from cod_prep.utils import report_if_merge_fail
from datetime import datetime


class RestrictionsCorrector(CodProcess):
    """Correct age and sex restrictions"""
    standard_cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_results': False,
        'cache_dir': "standard"
    }

    def __init__(self, code_system_id, cause_set_version_id, collect_diagnostics=False,
                 verbose=False, groupby_cols=['location_id', 'cause_id', 'code_id', 'year_id',
                                              'sex_id', 'age_group_id', 'nid', 'extract_type_id',
                                              'site_id']):
        self.code_system_id = code_system_id
        self.cause_set_version_id = cause_set_version_id
        self.collect_diagnostics = collect_diagnostics
        self.verbose = verbose
        self.groupby_cols = groupby_cols

        # perhaps set some logging stuff

    def get_computed_dataframe(self, df):
        """Return a dataframe with no age and sex restrictions"""
        # get restrictions mapping by cause
        if self.verbose:
            print("[{}] Prepping restrictions".format(str(datetime.now())))
        restrictions = self.prep_restrictions(self.cause_set_version_id)

        # run the more code system specific replacements
        if self.verbose:
            print("[{}] Setting restricted cause".format(str(datetime.now())))
        manual_restrictions = self.set_restricted_cause(df)
        # identify the restrictions
        if self.verbose:
            print("[{}] Marking restrictions".format(str(datetime.now())))
        merged_restrictions = self.identify_restrictions(manual_restrictions,
                                                         restrictions)
        # good place to save diagnostics
        if self.collect_diagnostics:
            self.diag_df = merged_restrictions.copy()

        # Replace the cause
        if self.verbose:
            print("[{}] Replacing cause".format(str(datetime.now())))
        # where the row represents a restriction violation, replace the code id
        # and the cause id with the restriction code/cause
        merged_restrictions.loc[
            merged_restrictions.restricted == 1,
            'code_id'] = merged_restrictions['restricted_code_id']
        merged_restrictions.loc[
            merged_restrictions.restricted == 1,
            'cause_id'] = merged_restrictions['restricted_cause_id']

        # then collapse
        # identify the restrictions
        if self.verbose:
            print("[{}] Collapsing".format(str(datetime.now())))
        merged_restrictions = merged_restrictions.groupby(
            self.groupby_cols, as_index=False)['deaths'].sum()

        return merged_restrictions

    def get_diagnostic_dataframe(self):
        """Return an evaluation of success of restrictions corrections

        Perhaps return a dataframe which is just the subset of the original
        that violated some age and sex restriction
        """
        if self.diag_df is None:
            raise AssertionError(
                "Need to run get_computed_dataframe once "
                "with collect_diagnostics = True")
        return self.diag_df

    def prep_restrictions(self, cause_set_version_id):
        """Get all the needed restrictions data

        Implemented by pulling from central get_causes function,
        then subsetting to just the restriction info for each cause_id.
        """
        df = get_current_cause_hierarchy(
            cause_set_version_id=cause_set_version_id,
            **self.standard_cache_options
        )
        df = df[['acause', 'cause_id', 'male',
                 'female', 'yll_age_start', 'yll_age_end']]
        return df

    def prep_code_metadata(self):
        df = get_cause_map(
            self.code_system_id,
            **self.standard_cache_options
        )
        df = df[['code_id', 'value', 'cause_id']]
        df = df.rename(columns={'value': 'raw_cause'})
        return df

    def set_restricted_cause(self, df):
        """Run a set of manual replacements, according to expert opinion."""

        # based on first letter of icd code, certain values chould be filled in
        mapping_icd10 = {'A': 'B99.9', 'B': 'B99.9', 'C': 'D49.9',
                         'D': 'D49.9', 'I': 'I99.9', 'J': 'J98.9',
                         'K': 'K92.9', 'V': 'Y89', 'Y': 'Y89'}

        # add value field
        df = add_code_metadata(
            df, ['value'], self.code_system_id,
            **self.standard_cache_options
        )
        report_if_merge_fail(df, 'value', 'code_id')
        df = df.rename(columns={'value': 'raw_cause'})

        # generate new column called "restricted_cause"
        # ZZZ is the default for all code systems
        raw_causes = self.prep_code_metadata()
        assert "ZZZ" in raw_causes.raw_cause.unique(), \
            "ZZZ must be in the map"
        df['restricted_cause'] = "ZZZ"
        df['restricted_code_id'] = raw_causes.query(
            "raw_cause == 'ZZZ'")["code_id"].values[0]
        df['restricted_cause_id'] = raw_causes.query(
            "raw_cause == 'ZZZ'")["cause_id"].values[0]

        # restrictions if code system is ICD10
        if self.code_system_id == 1:
            for key in mapping_icd10.keys():
                raw_cause = mapping_icd10[key]
                code_list = raw_causes.query(
                    "raw_cause == '{}'".format(raw_cause))
                assert len(code_list) == 1,  \
                    "Found more than one code with value {} in code " \
                    "system {}".format(raw_cause, self.code_system_id)
                new_code_id = code_list['code_id'].iloc[0]
                new_cause_id = code_list['cause_id'].iloc[0]
                df.loc[df['raw_cause'].str.startswith(key),
                       ['restricted_cause', 'restricted_code_id',
                        'restricted_cause_id']] = [raw_cause,
                                                   new_code_id, new_cause_id]
            # replace restricted_cause = "acause_diarrhea"
            # if inlist(yll_cause,"digest_ibd","digest_vascular")
            code_list = raw_causes.query('raw_cause == "acause_diarrhea"')
            assert len(code_list) == 1,  \
                "Found more than one code with value {} in code " \
                "system {}".format(raw_cause, self.code_system_id)
            new_code_id = code_list['code_id'].iloc[0]
            new_cause_id = code_list['cause_id'].iloc[0]
            # changes for digest_ibd
            df.loc[df['cause_id'] == 532,
                   ['restricted_cause']] = raw_cause
            df.loc[df['cause_id'] == 532,
                   ['restricted_code_id']] = new_code_id
            df.loc[df['cause_id'] == 532,
                   ['restricted_cause_id']] = new_cause_id
            # changes for digest_vascular
            df.loc[df['cause_id'] == 533,
                   ['restricted_cause']] = raw_cause
            df.loc[df['cause_id'] == 533,
                   ['restricted_code_id']] = new_code_id
            df.loc[df['cause_id'] == 533,
                   ['restricted_cause_id']] = new_cause_id
        # restrictions if code system is ICD9
        if self.code_system_id == 6:
            df['numeric_cause'] = pd.to_numeric(
                df['raw_cause'], errors='coerce')

            # 0-140 to 139.8
            new_code_id = raw_causes.query(
                "raw_cause == '139.8'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '139.8'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 1) & (df.numeric_cause < 140),
                ['restricted_cause', 'restricted_code_id',
                 'restricted_cause_id']
            ] = "139.8", new_code_id, new_cause_id

            # replace restricted_cause = "239.9" if numeric_cause >= 140
            # & numeric_cause < 240
            new_code_id = raw_causes.query(
                "raw_cause == '239.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '239.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 140) & (df.numeric_cause < 240),
                ['restricted_cause', 'restricted_code_id',
                 'restricted_cause_id']
            ] = "239.9", new_code_id, new_cause_id

            # replace restricted_cause = "459.9" if numeric_cause >= 390
            # & numeric_cause < 460
            new_code_id = raw_causes.query(
                "raw_cause == '459.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '459.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 390) & (df.numeric_cause < 460),
                ['restricted_cause', 'restricted_code_id',
                 'restricted_cause_id']
            ] = "459.9", new_code_id, new_cause_id

            # replace restricted_cause = "5199" if numeric_cause >= 460
            # & numeric_cause < 520
            new_code_id = raw_causes.query(
                "raw_cause == '519.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '519.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 460) & (df.numeric_cause < 520),
                ['restricted_cause', 'restricted_code_id',
                 'restricted_cause_id']
            ] = "519.9", new_code_id, new_cause_id

            # replace restricted_cause = "578" if numeric_cause >= 520
            # & numeric_cause < 580
            new_code_id = raw_causes.query(
                "raw_cause == '578'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '578'")["cause_id"].values[0]
            df.loc[(df.numeric_cause >= 520) & (df.numeric_cause < 580),
                   ['restricted_cause', 'restricted_code_id',
                    'restricted_cause_id']] = "578", new_code_id, new_cause_id

            # replace restricted_cause = "E989" if substr(cause,1,1) == "E"
            new_code_id = raw_causes.query(
                "raw_cause == 'E989'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == 'E989'")["cause_id"].values[0]
            df.loc[df['raw_cause'].str.startswith("E"),
                   ['restricted_cause', 'restricted_code_id',
                    'restricted_cause_id']] = "E989", new_code_id, new_cause_id
        assert pd.notnull(df.restricted_code_id).all()
        assert pd.notnull(df.restricted_cause_id).all()
        return df

    def identify_restrictions(self, df, restrictions):
        """Merge restrictions on data and mark violations."""
        merged_restrictions = df.merge(restrictions, on='cause_id')

        merged_restrictions = add_age_metadata(
            merged_restrictions,
            ['age_group_years_start', 'age_group_years_end'],
            **self.standard_cache_options
        )
        merged_restrictions.loc[(pd.isnull(merged_restrictions.yll_age_start)),
                                'yll_age_start'] = 0
        merged_restrictions.loc[
            (pd.isnull(merged_restrictions.male)), 'male'] = 1
        merged_restrictions.loc[(pd.isnull(merged_restrictions.female)),
                                'female'] = 1
        merged_restrictions.loc[merged_restrictions.yll_age_end == 95,
                                'yll_age_end'] = 125
        merged_restrictions.loc[merged_restrictions.age_group_years_start ==
                                0.01917808, 'age_group_years_start'] = .01
        merged_restrictions.loc[merged_restrictions.age_group_years_end ==
                                0.01917808, 'age_group_years_end'] = .01
        merged_restrictions.loc[merged_restrictions.age_group_years_start ==
                                0.07671233, 'age_group_years_start'] = .1
        merged_restrictions.loc[merged_restrictions.age_group_years_end ==
                                0.07671233, 'age_group_years_end'] = .1

        # outline and mark violations
        # use offset to make sure that if because of floating point weirdness
        # data age years start is 1.000001 and yll_age_end is 1.0000009,
        # data age years start doesn't break violation
        offset = .0001
        male_violation = ((merged_restrictions.sex_id == 1) &
                          (merged_restrictions.male == 0))
        fem_violation = ((merged_restrictions.sex_id == 2) &
                         (merged_restrictions.female == 0))
        young_violation = (merged_restrictions.age_group_years_start + offset <
                           merged_restrictions.yll_age_start)
        old_violation = (merged_restrictions.age_group_years_start - offset >
                         merged_restrictions.yll_age_end)
        violation = male_violation | fem_violation | \
            young_violation | old_violation

        merged_restrictions.loc[violation, 'restricted'] = 1

        return merged_restrictions

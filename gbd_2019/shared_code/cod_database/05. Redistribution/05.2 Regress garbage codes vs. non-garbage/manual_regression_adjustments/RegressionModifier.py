import pandas as pd
import sys
import os
import getpass
import numpy as np
import datetime
import cod_prep


class RegressionModifier(object):

    """
        TODO:
            - add method to parse weight group name for you
            - add method to zero out a cause/location/age/sex and scale
              up other causes
    """

    regressions_mod_folder = "FILEPATH"
    work_dir = str()
    shared_package_version_id = int()
    orig_props = pd.DataFrame()
    new_props = pd.DataFrame()
    verbose = bool()

    proportion_df_cols = [
        'shared_package_version_id',
        'shared_package_version_description',
        'shared_package_wgt_id',
        'shared_group_id',
        'target_codes',
        'shared_wgt_group_id',
        'wgt_group_name',
        'wgt'
    ]

    differences_file = "{work_dir}/differences.csv"

    upload_succeeded = False

    def __init__(self, shared_package_version_id, work_folder_name,
                 verbose=True):

        # set verbose flag
        self.verbose = verbose

        # set shared package version id
        self.shared_package_version_id = shared_package_version_id

        # make sure that concatenation of manual regressions folder and work
        # wolder is a path
        self.work_dir = "{}/{}".format(self.regressions_mod_folder,
                                       work_folder_name)
        if not os.path.isdir(self.work_dir):
            raise AssertionError(
                "{} is not a valid directory. did you: \n -make the folder "
                "in {}?\n -pass the folder name only instead of "
                "the full path?".format(self.work_dir,
                                        self.regressions_mod_folder)
            )

        # load the old package proportions into the class
        if self.verbose:
            print("Loading old package proportions...")
        self.load_old_package_proportions()
        assert len(self.orig_props) > 0, "Doesn't look liked {} is a valid " \
                                         "shared_package_version_id.".format(
                                             shared_package_version_id)

    def generate_new_proportions(self):
        """

        Requires:
            custom_modify_proportions is implemented using a new class
                that inherits this one

            e.g. a class for implementing a simple change that wouldn't happen,
                set all inj_homicide to 1 and all inj_war to 0:
                (dont worry about memory/reference problems,
                custom_modify_proportions will always receive a copy of
                a dataframe rather than the actual one)
            '
            class WarRegressionModifier(RegressionModifier)
                def custom_modify_proportions(self, prop_df):
                    prop_df.loc[prop_df['target_codes'] == "inj_homicide",
                               'wgt'] = 1
                    prop_df.loc[prop_df['target_codes'] == "inj_war",
                               'wgt'] = 0
                    return prop_df
            '
            And that's it! That would be all you'd need to do. for that part.

        Throws:
            AssertionError if proportion validation fails
            NotImplementedError if called from the base class
        """
        # set to result of custom modify proportions, restricted to required
        # columns
        old_prop_df = self.fetch_old_package_proportions()
        if self.verbose:
            print("Calling custom modification method...")
        new_props_staged = self.custom_modify_proportions(old_prop_df)

        assert self.new_props.shape == (
            0, 0), "Custom modify proportions should leave new_props alone"

        # validate the new proportions
        if self.verbose:
            print("Validating result...")
        self._validate_staged_proportions(new_props_staged)

        # then set the new proportions if that passes (that way you cant upload
        # invalid ones)
        self.new_props = new_props_staged[self.proportion_df_cols]

        # write any differences from last time
        print("Writing differences to {}, make sure to check this before "
              "uploading (but I can't tell if "
              "you did!)...".format(
                  self.differences_file.format(
                      work_dir=self.work_dir)))
        self._write_differences()

    def custom_modify_proportions(self, prop_df):

        # this method should be overridden in an inherited class, and should
        # modify the old proportions
        raise NotImplementedError(
            "Please create a new class that inherits the "
            "RegressionModifier class and creates a method "
            "called 'custom_modify_proportions' that takes "
            "two arguments: self and old_prop_df and returns "
            "a new dataframe with the same columns present, "
            "but modified weights. You can use the method "
            "'fetch_old_package_proportions' and experiment with"
            "that to develop your method"
        )

    def load_old_package_proportions(self):
        """Load up the old package proportions.

        Makes a big assumption that is only true for regressions.
        Assumes that weight group names are
        not repeated and that each target group only has one
        target code. This allows modification
        based on these columns. Without this assumption, more
        complicated techniques for weight/prop
        modification have to be developed.
        """
        df = cod_prep.downloaders.pull_shared_package_proportions(
            self.shared_package_version_id)
        # the database makes no guarantee of this. This suggests there are no
        # repeated weight groups, and each
        # target group has exactly one target code. It is a necessary
        # assumption for current methods of modifying
        # regression packages
        assert not df[['wgt_group_name', 'target_codes']
                      ].duplicated().values.any(), \
            "Assumption that weights are uniquely identified" \
            "by weight group name and target code" \
            " does not hold."
        # set to result, restricted on columns we need
        self.orig_props = df[self.proportion_df_cols]

    def fetch_path_to_work_dir(self):
        """Fetch path to work directory.

        Resources for custom modification might exist here, like new props
        or a list of causes to zero out.

        Args:
            None

        Returns:
            work_path, str: the path to the work directory
        """
        return self.work_dir

    def fetch_old_package_proportions(self):
        """Fetch the package proportions

        Args:
            None

        Raises:
            AssertionError if modify_proportions has not
                yet been run (then there are no new props)
        """
        return self.orig_props.copy()

    def fetch_new_package_proportions(self):
        """Fetch the package proportions

        Args:
            None

        Raises:
            AssertionError if modify_proportions has
                not yet been run (then there are no new props)
        """
        if self.new_props.shape == (0, 0):
            raise AssertionError(
                "New proportions have not been successfully generated "
                "using the modify_proportions method."
            )
        return self.new_props.copy()

    def update_proportions_in_engine_room(self):
        """Update each weight in the engine room with corresponding weight in prop_df.

        Will set the 'wgt' column in engine_room.rdp_sharedwgt
        to be what is contained
        in the prop_df using the 'shared_package_wgt_id' column.
        Both must be present.

        'shared_package_version_id', 'shared_group_id',
        and 'shared_wgt_group_id' are
        all used to verify integrity of the prop_df before
        uploading. Will check that they
        align with the shared_package_wgt_id that is in the
        prop_df, that they refer
        to a singular shared_package_version_id that is also
        the shared_package_version_id
        given in the prop_df, and that the 'wgt' column sums
        to 1 within each
        shared_wgt_group_id.

        Args:
            None (uses self.new_props)

        Returns:
            None

        Raises:
            AssertionError: New proportions have not been generated & validated
        """
        if self.new_props.shape == (0, 0):
            raise AssertionError(
                "New proportions have not beensuccessfully generated "
                "using the generate_new_proportions method."
            )

        upload_df = self.new_props[['shared_package_wgt_id', 'wgt']]
        upload_df = upload_df.set_index(
            'shared_package_wgt_id', verify_integrity=True)
        upload_dict = upload_df.to_dict()['wgt']

        if self.verbose:
            print("Updating {} rows in the rdp_sharedwgt table".format(
                len(upload_dict)))
        for shared_package_wgt_id in upload_dict.keys():
            new_wgt = upload_dict[shared_package_wgt_id]
            query = """
                UPDATE engine_room.rdp_sharedwgt
                SET wgt = {wgt}
                WHERE shared_package_wgt_id = {id}
            """.format(wgt=new_wgt, id=shared_package_wgt_id)
            if self.verbose:
                print(query)
            cod_prep.utils.queryToDF(
                query,
                select=False,
                host='internal-db-p02'
            )
        self.upload_succeeded = True
        print("Updated, marking best versions")
        self._update_best_version()
        print("Done!")

    def _update_best_version(self):
        """Update version of old to old and new to best"""
        assert self.upload_succeeded, "Upload has not successfully completed."
        shared_package_version_id = self.shared_package_version_id

        package_id = cod_prep.utils.queryToDF("""
            SELECT shared_package_id FROM engine_room.rdp_sharedpackageversion
            WHERE shared_package_version_id = {};
        """.format(shared_package_version_id),
            host='internal-db-p02').iloc[0, 0]

        curr_best_version_id = cod_prep.utils.queryToDF("""
            SELECT shared_package_version_id
            FROM engine_room.rdp_sharedpackageversion
            WHERE shared_package_id = {}
            AND shared_package_version_status_id = 2;
        """.format(package_id), host='internal-db-p02').iloc[0, 0]

        update_status_end = """
            UPDATE engine_room.rdp_sharedpackageversion
            SET end_date = '{dbt}'
            WHERE shared_package_version_id = {o_spvid}
        """

        update_status_old = """
            UPDATE engine_room.rdp_sharedpackageversion
            SET shared_package_version_status_id = 1
            WHERE shared_package_version_id = {o_spvid}
        """

        update_status_start = """
            UPDATE engine_room.rdp_sharedpackageversion
            SET start_date = '{dbt}'
            WHERE shared_package_version_id = {n_spvid}
        """

        update_status_new = """
            UPDATE engine_room.rdp_sharedpackageversion
            SET shared_package_version_status_id = 2
            WHERE shared_package_version_id = {n_spvid}
        """

        if curr_best_version_id != shared_package_version_id:
            # if another is active best, update the versioning
            db_timestamp = '{:%Y-%m-%d %H:%M:%S.%f}'.format(
                datetime.datetime.now())
            print("Running: \n{}".format(update_status_end.format(
                dbt=db_timestamp, o_spvid=curr_best_version_id)))
            # update status end of old version id
            cod_prep.utils.queryToDF(
                update_status_end.format(
                    dbt=db_timestamp, o_spvid=curr_best_version_id),
                select=False,
                host='internal-db-p02'
            )
            print("Running: \n{}".format(
                update_status_old.format(o_spvid=curr_best_version_id)))
            # update status of old version id to locked
            cod_prep.utils.queryToDF(
                update_status_old.format(o_spvid=curr_best_version_id),
                select=False,
                host='internal-db-p02'
            )
            print("Running: \n{}".format(update_status_start.format(
                dbt=db_timestamp, n_spvid=shared_package_version_id)))
            # update status start of new version id
            cod_prep.utils.queryToDF(
                update_status_start.format(
                    dbt=db_timestamp, n_spvid=shared_package_version_id),
                select=False,
                host='internal-db-p02'
            )
            print("Running: \n{}".format(update_status_new.format(
                n_spvid=shared_package_version_id)))
            # update status of new version id to best
            cod_prep.utils.queryToDF(
                update_status_new.format(n_spvid=shared_package_version_id),
                select=False,
                host='internal-db-p02'
            )

    def _validate_staged_proportions(self, test_df):
        """Ensures new proportions fit some standards.

        Tests that:
            * Expected columns are present
            * All proportions sum to 1 within each weight group in the dataset
            * The proportions are not exactly the same as before
            * Every proportion id matches one in the old proportions

        Args:
            df: pandas DataFrame to test

        Throws:
            KeyError if not all columns present
            AssertionError if not all weight groups
                sum to 1, proportions are exactly same,
                or there are weight ids that were not in old
        """
        # throws key error if not all columns present
        df = test_df[self.proportion_df_cols].copy()

        # test that weight groups sum to 1
        wgt_sums = df.groupby('wgt_group_name', as_index=False)['wgt'].sum()
        if not np.allclose(wgt_sums['wgt'], 1):
            print("Not all of these weight groups"
                  "sum to 1: \n{}".format(wgt_sums))

        # all new match an old
        df_old = self.fetch_old_package_proportions(
        )[['shared_package_wgt_id', 'wgt']]
        df_comp = df.merge(
            df_old, how='left',
            on='shared_package_wgt_id', suffixes=('_new', '_old'))
        assert df_comp.wgt_old.notnull().values.all(
        ), "Some weight group ids dont exist in the old"

        # not exactly the same as old ones
        if np.allclose(df_comp['wgt_new'], df_comp['wgt_old']):
            raise AssertionError(
                "There should be at least one difference in the new "
                "and old weights, otherwise.. why?"
            )

    def _write_differences(self):
        """Write a csv showing the change in weights between new and old.

        Shows the new weight, old weight, pct difference over old by
        weight group name and target code.

        Args:
            None

        Raises:
            None
        """
        # grab the old proportions, only weight id is necessary
        df_old = self.fetch_old_package_proportions(
        )[['shared_package_wgt_id', 'wgt']]
        # grab the new ones
        df_new = self.fetch_new_package_proportions()

        df_new = df_new[['wgt_group_name', 'target_codes',
                         'shared_package_wgt_id', 'wgt']]
        # this merge will work if validate new props passed (not the time to
        # test this)
        df_comp = df_new.merge(
            df_old, how='left',
            on='shared_package_wgt_id', suffixes=('_new', '_old'))
        #
        df_comp['pct_diff'] = abs(
            df_comp['wgt_old'] - df_comp['wgt_new']) / df_comp['wgt_old']
        # save the differences to the work directory
        df_comp = df_comp.query('wgt_new != wgt_old')
        df_comp.to_csv(self.differences_file.format(work_dir=self.work_dir))


class ZeroOutRegressionModifier(RegressionModifier):

    zero_acause = ""

    def set_zero_acause(self, acause):
        self.zero_acause = acause

    def custom_modify_proportions(self, prop_df):
        assert self.zero_acause != "", "Must set which cause to zero out"

        # zero out this cause
        zero_out = 1 * (prop_df['target_codes'] == self.zero_acause)

        # multiply china, meso weights by 0
        prop_df['new_wgt'] = prop_df['wgt'] * (1 - zero_out)

        # scale up
        # scaling up china weights where it is non-meso
        prop_df['scale_up'] = 1 - zero_out

        # sum up the residuals only (scale them up to 1 - sum of fixed weights)
        # fixed weights are the weights for which scale_up == 0
        prop_df['residual_wgt'] = prop_df[
            'new_wgt'] * prop_df['scale_up']
        prop_df['residual_sum'] = prop_df.groupby(
            'wgt_group_name')['residual_wgt'].transform(np.sum)
        # these weights get scaled up to wgt/residual sum so
        # that the sum of new_wgt
        # where scale_up == 1 is equal to residual_sum
        prop_df.loc[prop_df['scale_up'] == 1, 'new_wgt'] = prop_df[
            'new_wgt'] / prop_df['residual_sum']

        assert np.allclose(prop_df.groupby(
            'wgt_group_name').wgt.sum(), 1)
        assert np.allclose(prop_df.groupby(
            'wgt_group_name').new_wgt.sum(), 1)
        prop_df = prop_df.drop(
            ['scale_up', 'residual_wgt', 'residual_sum'], axis=1)
        # check that no weights are null
        assert prop_df['new_wgt'].notnull().values.all()

        # swap the new weight with the old
        prop_df = prop_df.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # return
        return prop_df


class DirectRegressionModifier(RegressionModifier):
    # RegressionModifier for the case where proportions are directly modified
    # using a csv with a column 'wgt_group_name' with valid wgt group names
    # and each target in the regression as columns

    def custom_modify_proportions(self, prop_df):
        new_weights = pd.read_csv(
            "{}/extracted_weights.csv".format(self.work_dir))

        # reshape long
        assert not new_weights['wgt_group_name'].duplicated().any()
        new_weights = new_weights.set_index("wgt_group_name").unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})

        # make sure that each weight group name in the new weights is in
        # the old weights
        missing_names = set(new_weights['wgt_group_name']) - \
            set(prop_df['wgt_group_name'])
        if missing_names != set():
            raise AssertionError(
                "These weight group names in the extracted weights file"
                "are not in the old weights: {}".format(missing_names)
            )

        # make sure that all target groups in the extracted weights
        # exactly match those in the old weights
        only_new = set(new_weights['target_codes']) - \
            set(prop_df['target_codes'])
        only_old = set(prop_df['target_codes']) - \
            set(new_weights['target_codes'])
        if only_old != set():
            raise AssertionError(
                "These targets not in new: {}".format(only_old)
            )
        if only_new != set():
            raise AssertionError(
                "These targets not in old: {}".format(only_new)
            )

        # merge the new weights in and verify all new weights have a match
        new_df = new_weights.merge(prop_df, how='left')
        assert new_df['wgt'].notnull().values.all()

        # make sure weights sum to 1
        assert np.allclose(new_df.groupby(
            'wgt_group_name').new_wgt.sum(), 1)

        # swap the new weight with the old
        new_df = new_df.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # set the new props attribute, restricting to columns we need to upload
        return new_df

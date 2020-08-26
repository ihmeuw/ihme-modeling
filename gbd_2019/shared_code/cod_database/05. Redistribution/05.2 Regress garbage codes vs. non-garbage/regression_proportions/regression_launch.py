import getpass
import sys
import subprocess

import pandas as pd

from cod_prep.utils import (
    cod_timestamp,
    get_git_commit_hash,
    get_git_modified_files,
    print_log_message
)

import prep_proportions_data
import save_proportions_for_tableau


DIR = "FILEPATH"
REGRESSION_SCRIPT = "run_redistribution_regression.R"


class RegressionLauncher(object):
    """RegressionLauncher handles the versioning and running of regressions
    for the purpose of creating redistribution proportions for a select group
    of shared packages. It also handles the versioning for the input data and
    square datasets (for predictions) that go into those regressions.

    Regression launch sets consist of all parameters that make up a unique run
    of a regression, identified by a regression launch set it (RLSID). It is
    an idea based on the claude launch set. The fields (as of 12/7/18) are:
        - regression_launch_set_id
        - run_description: string description of the regression run,
            for transparency
        - shared_package_id
        - shared_package_name
        - formula: regression formula (currently linear)
        - data_id: unique id for the input data
        - data_description: string description for the data version
        - commit_hash: hash value for the most recent git commit
        - timestamp
        - username: who ran the regression (read: who to blame)

    Instantiation: RegressionLauncher only needs enough information as
    necessary to build out a regression launch set based on previous sets;
    every parameter does not need to be passed in each time. For example, to
    run a regression on cancer using the exact same data but a slightly
    different formula, all you need to pass in is the old RLSID to copy
    and the new formula and RegressionLauncher will fill in the rest of
    the details.

    init arguments:
        - regression_launch_set_id: old RLSID to fill in gaps with
        - shared_package_id
        - shared_package_name (only need this if don't have a RLSID or a SPID)
        - formula: linear regression formula, as a string
        - run_description
        - data_id/data_description: only one can be passed in. If data_id,
            then that data version is used. If data description, a fresh
            copy of data is downloaded and cataloged.
        - test: drops the check for commited files

    Included shared packages (as of 12/7/18): diabetes unspecified,
    stroke unspecified, cancer unspecified (multiple)

    All regression launch sets are save in a csv currently, but should
    be moved into a database at some point if this is continued.
    """

    def __init__(self, regression_launch_set_id=None,
                 shared_package_id=None, shared_package_name=None,
                 formula=None, run_description=None, data_id=None,
                 data_description=None, test=False):
        # Assert arguments passed in are valid to give flexibility in
        # creating a RegressionLauncher
        if run_description is None:
            raise AssertionError("Argument 'run_description' missing.")

        if data_id is not None and data_description is not None:
            # If both data_id and data_description were supplied,
            # that's an issue! We either use an old data version (data_id
            # supplied) or we give new data description and create new data
            raise AssertionError("Only one of the following arguments may be supplied: "
                                 "'data_id', 'data_description'. Supply 'data_id' to "
                                 "use an old data version. Supply 'data_description' to "
                                 "create a new set of data.")

        if regression_launch_set_id is None:
            # If the RLSID isn't passed in, all necessary arguments must
            # be supplied by user!
            for arg in [shared_package_id, formula]:
                assert arg is not None, "Missing needed argument without an " \
                                        "explicit regression_launch_set_id " \
                                        "('shared_package_id', 'formula')."
            assert data_id is not None or data_description is not None, "Either 'data_id' or data_description' must be supplied if no RLSID is."

        self._test = test
        if not self._test:
            self.assert_repo_is_committed()
        else:
            print("***TEST*** - Not checking script commit status")

        self._launch_set_dict = {
            "regression_launch_set_id": RegressionLauncher.new_launch_set_id(),
            "username": getpass.getuser(),
            "commit_hash": get_git_commit_hash(),
            "timestamp": cod_timestamp(),
            "run_description": run_description,
            "formula": formula,
            "shared_package_id": shared_package_id,
            "shared_package_name": shared_package_name,
            "data_id": data_id,
            "data_description": data_description
        }

        self.fill_in_launch_set_gaps(regression_launch_set_id)

    def upload_launch_set(self):
        """Upload the launch_set_id to the launch_set_table.
        Ensures colunms stay in the right order
        """
        try:
            RegressionLauncher.get_launch_set(self._launch_set_dict["regression_launch_set_id"])
        except LookupError:
            pass  # RLSID doesn't exist, move on
        else:
            raise AssertionError("Regression launch set id {} already exists.".format(
                self._launch_set_dict["regression_launch_set_id"]))

        table = RegressionLauncher.get_launch_table()
        table = table.append(pd.DataFrame(self._launch_set_dict, index=[0]))
        table = table[["regression_launch_set_id", "run_description",
                       "shared_package_id", "shared_package_name",
                       "formula", "data_id", "data_description",
                       "commit_hash", "timestamp", "username"]]
        table.to_csv(
            "{DIR}/regression_launch_set_id_table.csv".format(DIR=DIR), index=False)

    """ STATIC (AKA INSTANTIATION FREE) METHODS"""
    @staticmethod
    def get_launch_table(shared_package_id=None):
        """Returns the regression launch set id table.
        Optional shared_package_id argument
        allows subsetting table to just rows for the given shared_package
        """
        table = pd.read_csv("{DIR}/regression_launch_set_id_table.csv".format(DIR=DIR))

        if shared_package_id is not None:
            table = table.loc[table['shared_package_id'] == shared_package_id]

        return table

    @staticmethod
    def new_launch_set_id():
        """Returns the next regression launch set ID by adding 1 to
        the most recent ID. Currently in a .csv but should probably
        move to a database at some point
        """
        rlsid_table = RegressionLauncher.get_launch_table()
        return rlsid_table[["regression_launch_set_id"]].iloc[-1].item() + 1

    @staticmethod
    def get_launch_set(regression_launch_set_id):
        """Use given regression_launch_set_id to grab the
        regression launch set.
        """
        rlsid_table = RegressionLauncher.get_launch_table()
        launch_set = rlsid_table[rlsid_table.regression_launch_set_id ==
                                 regression_launch_set_id]
        if len(launch_set) != 1:
            raise LookupError("The regression_launch_set_id given ({}) does "
                              "not exist or is not unique. Returns {} "
                              "rows".format(regression_launch_set_id, len(launch_set)))

        return launch_set

    """ GETTERS """

    def get_shared_package_name(self):
        table = RegressionLauncher.get_launch_table(self._launch_set_dict["shared_package_id"])

        if len(table) > 0:
            return table["shared_package_name"].iloc[0]
        elif self._launch_set_dict["shared_package_name"] is None:
            raise IndexError(
                "Shared package ID {} does not exist in table. Please provide "
                "a 'shared_package_id' to continue.".format(shared_package_id))
        else:  # shared_package_name already provided
            return self._launch_set_dict["shared_package_name"]

    def get_data_description(self):
        """Return the data description for the given data_id, given
        a shared_package_id. Assumes shared_package_id is already in
        self._launch_set_dict
        """
        table = RegressionLauncher.get_launch_table(self._launch_set_dict["shared_package_id"])
        data_id = self._launch_set_dict["data_id"]
        table = table.loc[(table['data_id'] == data_id)]

        if len(table) > 0:
            return table["data_description"].iloc[0]
        else:
            raise IndexError(
                "Data ID {} does not exist in table.".format(data_id))

    def fill_in_launch_set_gaps(self, regression_launch_set_id):
        """Fill in the gaps of a regression launch set with the values
        from the regression launch set id passed in. If None, only fills in
        shared_package_name and data_description (or downloads new data)
        """
        self._data_decription_given = False

        if self._launch_set_dict["data_description"] is not None:
            self._data_decription_given = True

        if regression_launch_set_id is not None:
            launch_set = RegressionLauncher.get_launch_set(
                regression_launch_set_id)
            for key, value in self._launch_set_dict.iteritems():
                if value is None:
                    self._launch_set_dict[key] = launch_set[key].item()

        self._launch_set_dict["shared_package_name"] = self.get_shared_package_name(
        )

        if not self._data_decription_given:
            # Case 1: data_id or past RLSID supplied, so keep that info
            # to overwrite data description from RLSID
            self._launch_set_dict["data_description"] = self.get_data_description()
        else:
            # Case 2: data description given, no data id means get new data
            self.download_fresh_data()

    """ MISC? """

    def download_fresh_data(self):
        """If only a data description is passed in,
        download and manipulate current set of data
        so that the regression input data is up to date.

        Increments the highest data_id by 1 to create the new
        data_id. Uses prep_proportions_data.py to download data
        """
        table = RegressionLauncher.get_launch_table(
            self._launch_set_dict["shared_package_id"])

        if len(table) > 0:
            self._launch_set_dict["data_id"] = table['data_id'].max() + 1
        else:  # new shared pakcage, no previous data
            self._launch_set_dict["data_id"] = 0

        prep_proportions_data.main(shared_package_id=self._launch_set_dict["shared_package_id"],
                                   data_id=self._launch_set_dict["data_id"],
                                   test=self._test)

    def assert_repo_is_committed(self):
        """Make sure you have committed everything you should have.

        Reason: we are storing the commit hash of each regression run,
        and for your repo to be representative of the code that was
        run, you need to commit your code before running.
        """
        mod_files = get_git_modified_files()
        if mod_files != "":
            raise AssertionError(
                "Please commit or stash these files before "
                "running: \n{}".format(mod_files)
            )

    def run_regression(self, force_rerun=False):
        # only upload to the RLSID table iff this is a fresh run.
        # sometimes I need to hack the run script or theres a simply error, etc
        # so it makes sense to be able to force a rerun after fixing things
        # NOTE: force_rerun=True will run the R script as is
        if not force_rerun:
            self.upload_launch_set()

        print("Running regression on shared_package_id {a} with formula {b}".format(
            a=self._launch_set_dict["shared_package_id"],
            b=self._launch_set_dict["formula"]))
        errno = subprocess.call("FILEPATH".format(
                                REGRESSION_SCRIPT), shell=True)

        if errno:
            raise RuntimeError("{} had a non-zero exit status: {}".format(REGRESSION_SCRIPT, errno))

    @staticmethod
    def save_proportions_for_tableau():
        save_proportions_for_tableau.main()

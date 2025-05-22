"""
To Use:

test = ValidateExtractions()  #instantiate
test._get_expected_dimensions(2010)  #run the test that reads all lines
test.failures  # review failed files

test.run_extraction_test()  # run full test on all years
"""

import re
import glob
import os

import pandas as pd


class ValidateExtractions(object):
    """
    After running every modified SAS script we need to test the outputs to make sure
    all the files are there, and there weren't any silent/unrecognized errors and
    that the csv dimensions match what we'd expect
    """

    def __init__(self, request_id, run_now=False):
        # set a few properties we'll re-use
        self.request_id = request_id
        self.parent_dir = "FILEPATH"
        self.years = [2000, 2010, 2014, 2015, 2016]
        self.size_floor = 1.5e7  # 15Mb floor on files
        self.failures = []  # the tests that fail
        self.verbose = False
        self._set_dir_regex

        if run_now:
            # For complete automation, ie it runs tests on instantiation
            self.run_extraction_tests()
        else:
            pass

    def run_extraction_tests(self):
        """run the other functions below to check outputs from the
        modified SAS scripts"""

        for year in self.years:
            self._compare_scripts_to_files(year)
            self._confirm_csv_size(year)
            self._get_expected_dimensions(year)

        assert not self.failures, f"{self.failures}"

        return "the year(s) {} look fine".format(", ".join([str(y) for y in self.years]))

    @property
    def _set_dir_regex(self):
        self.dir_regex = {'9437': {'data_regex': "FILEPATH",
                                 'script_regex': "FILEPATH"},
                          '8140': {'data_regex': "FILEPATH",
                                 'script_regex': "FILEPATH"}
                         }
        self.dir_regex = self.dir_regex[self.request_id]

    def _get_expected_dimensions(self, year, check_subset=[]):
        """The .fts files contain info on columns and row counts that we can use to
        confirm that the CSV files we've extracted match what we expect

        Params:
            year: (int) Normally this function loops over all the .fts files for a given year
                        checking each one against the data present

            check_subset: (list) if you're running interactively and don't want to wait while
                                 all files for a given year are loaded then pass a list of .fts
                                 files to this arg to check just them
        """

        assert isinstance(
            check_subset, list), "Please make check_subset a list of fts files"
        if check_subset:
            fts_files = check_subset
            msg = "Checking a subset of {} of the .fts files and data".format(
                len(check_subset))
        else:
            fts_files = glob.glob("FILEPATH")
            msg = "Checking the fts files and data for {}".format(year)

        print(msg)
        for f in fts_files:
            if self.verbose:
                print(os.path.basename(f)[:-32])
            try:
                with open(f, 'r') as file:
                    filedata = file.read()
                    exp_rows = re.search(
                        "Exact File Quantity \(Rows\).+", filedata)
                    # the header
                    exp_rows = int(re.sub("[^0-9]", "", exp_rows.group(0))) + 1
                    exp_cols = re.search("Columns in File.+", filedata)
                    exp_cols = int(re.sub("[^0-9]", "", exp_cols.group(0)))
                csv_name = os.path.basename(f)[:-4]
                if self.request_id == '8140':
                    full_csv_path = "FILEPATH"
                elif self.request_id == '9437':
                    p = re.compile(".+_k|.+_codes")
                    modc = p.search(csv_name).group(0)
                    full_csv_path = "FILEPATH"
                # check obs columns
                tmp = pd.read_csv(full_csv_path, nrows=5)
                if tmp.shape[1] != exp_cols:
                    msg = "Expected {} columns but got {} in file {}".format(exp_cols, tmp.shape[1], f)
                    self.failures.append(msg)

                with open(full_csv_path, encoding='latin-1') as csv:
                    ob_rows = sum(1 for line in csv)
                if ob_rows != exp_rows:
                    msg = "Expected {} rows but got {} in file {}".format(exp_rows, ob_rows, f)
                    self.failures.append(msg)
                if self.verbose:
                    print(ob_rows, tmp.shape[1])
                    print(exp_rows, exp_cols)

            except Exception as e:
                print("something went wrong in file {}".format(f), e)
                self.failures.append(f + str(e))

        return

    def _compare_scripts_to_files(self, year):
        """Confirm that the files on drive are the same as the sas scripts that were modified"""

        # use this list of scripts to check against
        if self.request_id == '8140':
            idx_start = 9
        elif self.request_id == '9437':
            idx_start = 0
        scripts = [os.path.basename(f)[idx_start:-12] for f in
                     glob.glob("FILEPATH")]

        # pull the files we actually wrote
        if self.request_id == '8140':
            idx_end = 32
        elif self.request_id == '9437':
            idx_end = 4
        files = [os.path.basename(f)[0:-idx_end] for f in
                 glob.glob("FILEPATH")]

        if not scripts or not files:
            print("Either sas scripts or output files or both were identified by this method")
        diff = set(scripts).symmetric_difference(set(files))
        if diff:
            msg = f"There are missing files, here is the set symmetric difference: {diff}"
            self.failures.append(msg)
        print("Completed tests for differences between for year {}".format(year))
        return

    def _confirm_csv_size(self, year):
        """set a min file size, with a few exceptions at 15 megs"""

        files = glob.glob("FILEPATH")
        for f in files:
            if 'span' not in f:
                if 'demo' not in f:
                    if f == "FILEPATH":
                        continue
                    file_size = os.path.getsize(f)
                    if file_size <= self.size_floor:
                        msg = "{} failed the size test".format(f)
                        self.failures.append(msg)

        print("Completed confirm csv size test; {}".format(year))
        return

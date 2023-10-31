# process claims data
import subprocess
import time
import shutil
import warnings
import getpass
import os

from clinical_info.Claims.query_ms_db.submit_process_marketscan import run_marketscan
from clinical_info.Mapping import clinical_mapping
from clinical_info.Marketscan.process_db_data.create_bundle_to_rei_acause_map import write_acause_rei_to_bundle_map
from clinical_info.Claims.format import poland_master
from clinical_info.Functions import hosp_prep
from clinical_info.NoiseReduction import prep_stata_ms, submit_marketscan_nr

USER = getpass.getuser()
REPO = FILEPATH
"""

"""


class Claims(object):
    """ An object to control and run the claims process.


    Example use

    claim = Claims(run_id=1, nr_code='python')
    claim.main()

    Parameters
    ----------
    run_id : int
        The id for the run.
    nr_code : str
        Which code language should be used to run noise reduction. 'stata' or 'python'
        Note that for GBD2020 and forward we expected stata to be deprecated
    write_results : bool
        Should results be written?
    repo : string
        Optional. The default repo is generated when claims.py is run.

    Attributes
    ----------
    map : str
        Default: None
    base : str
        The base path for the run.
    run_id
    write_results
    repo

    """

    def __init__(self, run_id, nr_code, decomp_step, write_results=True, repo=REPO,
                 run_poland=False, map_version="current"):
        self.map = None
        self.run_id = run_id
        self.nr_code = nr_code
        self.base = FILEPATH
        self.write_results = write_results
        self.repo = repo
        self.run_poland = run_poland
        self.decomp_step = decomp_step
        self.gbd_round_id = None
        self.map_version = map_version

    def __str__(self):
        display = "run_id is {}".format(self.run_id)
        display += "\nNoise reduction type is {}".format(self.nr_code)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\ncode from {}".format(self.repo)
        return display

    def __repr__(self):
        display = "run_id is {}".format(self.run_id)
        display += "\nNoise reduction type is {}".format(self.nr_code)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\ncode from {}".format(self.repo)
        return display

    def copy_files(self):
        shutil.copy(src=FILEPATH,
                    dst=FILEPATH.format(self.run_id))
        return

    def create_bundle_file(self):
        """Create a bundle file at:
            FILEPATH

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        df = clinical_mapping.get_clinical_process_data(
            'icg_bundle', prod=True)
        mdf = clinical_mapping.get_clinical_process_data(
            'icg_durations', prod=True)
        mdf = mdf.drop(['icg_name', 'icg_duration',
                        'map_version'], axis=1).drop_duplicates()
        df = df.merge(mdf, how='left', on='icg_id')

        prev_bundles = [543, 544, 545, 3260, 6707, 6998, 7001]
        warnings.warn(
            "We're forcing the bundles {} to have measure prevalence for the modeler".format(prev_bundles)
        df.loc[df.bundle_id.isin(prev_bundles), 'icg_measure'] = 'prev'
        df = df[['bundle_id', 'icg_measure']].drop_duplicates()
        df = df.drop_duplicates()
        assert df[df.bundle_id.duplicated(keep=False)].shape[0] == 0,\
            "Bundles have duped icg measures"
        df.rename(columns={'icg_measure': 'measure'}, inplace=True)
        df['measure'] = df['measure'].astype(str)

        stata_path = FILEPATH
        df.to_stata(stata_path, write_index=False)
        return

    def pre_nr_agg(self):
        """Submits the SCRIPT job
        to the cluster with switches 1 0 0 0 0 1 and the current run id.

        Parameters
        ----------


        Returns
        -------
        None

        """
        qs = QSUB.format(repo=self.repo, rid=self.run_id)

        print(qs)
        subprocess.call(qs, shell=True)
        return

    def ms_nr(self, facilities):
        """Submits the FILEPATH script to
        the cluster. The script uses switches generated form the passed in
        facilities and the current run id.

        Parameters
        ----------
        facilities : str
            facilities must be inp or all.

        Returns
        -------
        type
            Description of returned object.

        """
        if facilities == 'inp':
            switches = "0 1 1 0 0 1"
        elif facilities == 'all':
            switches = "0 1 0 0 0 1"
        else:
            assert False, "What are we trying to do here?"

        qs = QSUB.\
             format(repo=self.repo, swi=switches, rid=self.run_id)

        print(qs)
        subprocess.call(qs, shell=True)

    def write_bundle_csv(self):
        """Submits the FILEPATH script to the
        cluster using the current repo and run id.

        Parameters
        ----------  


        Returns
        -------
        None

        """

        """
        Once the claims process finishes we can prep final bundle estimates for upload
        """
        qsub = QSUB.\
            format(repo=self.repo, step=self.decomp_step,
                   r=self.run_id, nc=self.nr_code, mv=self.map_version)
        subprocess.call(qsub, shell=True)
        return

    def get_failed_numbers(regex_pattern, run_id):
        """
        Finds any missing group numbers in specific directory.

        This function is not called from the rest of the class, it's just a
        helper function the user can call to check exactly which ms_group files
        are missing

        Parameters
        ----------
        regex_pattern : str
            A regex pattern to match filenames. Used to select either bundle
            or icg files. should look like "bundle_group_\d{1,3}\." or
            "icg_group_\d{1,3}\."
        run_id : int
            current run_id

        Returns
        ----------
        set of group numbers missing from base_dir
        """

        base_dir = FILEPATH

        file_list = [f for f in os.listdir(
            base_dir) if os.path.isfile(os.path.join(base_dir, f))]

        regex_matches = [re.findall(
            pattern=regex_pattern, string=f) for f in file_list]

        regex_matches = [f for f in regex_matches if len(f) > 0]

        regex_matches = [f[0] for f in regex_matches]

        regex_matches = [re.findall(
            pattern="\d{1,3}\.", string=f) for f in regex_matches]

        group_numbers = [int(f[0].replace(".", "")) for f in regex_matches]

        group_numbers = list(set(group_numbers))

        all_groups = list(range(0, 350))

        return set(all_groups) - set(group_numbers)

    def main(self, run_step=0):
        """ Main method that controls the flow claims flow.

        main runs all steps greater than the passed in `run_step`

        Step 0: create a list of bundle IDs
        Step 1: run the long Marketscan process to create estimates and data for correction factors
        Step 2: aggregate data for NR
        Step 3: run all Python NR OR run Stata inpatient NR (MS claims run finished if NR == python)
        Step 4: run Stata union of inp and otp NR (pass if NR == python)
        Step 5: run Poland if applicable

        Examples:
        run_step = 2:
        Step 2, 3, 4, and possibly 5 will be run.

        Parameters
        ----------
        run_step : int
            The lower bound on steps to run.

        Returns
        -------
        None

        """

        self.copy_files()

        if run_step < 1:
            print("Creating a dta file with bundles and measure for the legacy stata process. Also an rei/acause map to bundle_id")
            self.create_bundle_file()
            write_acause_rei_to_bundle_map(self.run_id)

        if run_step < 2:
            print(
                "Sending out the master script to process MS data to the individual level")
            run_marketscan(self.run_id)
            hosp_prep.job_holder(job_name="ms_group", sleep_time=65)

        if run_step < 3:
            print("Qsubbing the legacy Stata code to aggregate before noise reduction")
            self.pre_nr_agg()
            hosp_prep.job_holder(job_name="pre_nr_", sleep_time=65)
            if self.nr_code == 'python':
                print("Prepping the data input for python Noise Reduction")
                prep_stata_ms.write_csv_for_python_nr(self.run_id)

        if run_step < 4:
            if self.nr_code == 'stata':
                print(
                    "Qsubbing the legacy Stata code to run Noise Reduction on inpatient data")
                self.ms_nr("inp")
                hosp_prep.job_holder(job_name="stata_submit", sleep_time=65)
                hosp_prep.job_holder(job_name="ms_nr", sleep_time=65)
            elif self.nr_code == 'python':
                print(
                    "Qsubbing the updated Python code to run Noise Reduction on inpatient data")
                submit_marketscan_nr.main(self.run_id, drops=[])
            else:
                raise ValueError(
                    f"NR code {self.nr_code} is not recognized must be 'stata' or 'python'")

        if run_step < 5:
            if self.nr_code == 'stata':
                print(
                    "Qsubbing the legacy Stata code to run Noise Reduction on U(inpatient, outpatient) data")
                self.ms_nr("all")
                hosp_prep.job_holder(job_name="stata_submit", sleep_time=65)
                hosp_prep.job_holder(job_name="ms_nr", sleep_time=65)
            else:
                pass

        if run_step < 6:
            if self.run_poland:
                self.gbd_round_id = 7
                self.decomp_step = 'step2'
                poland_master.main(n_groups=1000, run_id=self.run_id,
                                   round_id=self.gbd_round_id, step=self.decomp_step,
                                   cause='bundle', groups=False)

        print("Sending out the job to format the final bundle level estimates for upload.")
        self.write_bundle_csv()
        return

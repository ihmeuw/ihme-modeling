
import sys
import getpass
import subprocess
import time
import shutil
import warnings

user = getpass.getuser()
repo = "FILEPATH".format(user)
for p  in ["FILEPATH", "FILEPATH", "FILEPATH"]:
    sys.path.append(repo + p)

sys.path.append("FILEPATH".format(repo, user))
from submit_process_marketscan import run_marketscan
import clinical_mapping
from create_bundle_to_rei_acause_map import write_acause_rei_to_bundle_map

"""

"""
class Claims(object):
    """ An object to control and run the claims process.


    Example use

    claim = Claims(run_id='test_3')
    claim.main()

    Parameters
    ----------
    run_id : int
        The id for the run.
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
    def __init__(self, run_id, write_results=True, repo=repo):
        self.map = None
        self.run_id = run_id
        self.base = "FILEPATH".format(self.run_id)
        self.write_results = write_results
        self.repo = repo
        self.decomp_step = ""





    def __str__(self):
        display = "run_id is {}".format(self.run_id)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\ncode from {}".format(self.repo)
        return display

    def __repr__(self):
        display = "run_id is {}".format(self.run_id)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\ncode from {}".format(self.repo)
        return display

    def copy_files(self):
        """
        Copies files from FILEPATH
        """
        shutil.copy(src="FILEPATH",
                    dst="FILEPATH".format(self.run_id))
        return


    def create_bundle_file(self):
        """Create a bundle file at:
            `FILEPATH`

        Parameters
        ----------
        None

        Returns
        -------
        None

        """


        df = clinical_mapping.get_clinical_process_data('icg_bundle', prod=True)
        mdf = clinical_mapping.get_clinical_process_data('icg_durations', prod=True)
        mdf = mdf.drop(['icg_name', 'icg_duration', 'map_version'], axis=1).drop_duplicates()
        df = df.merge(mdf, how='left', on='icg_id')

        prev_bundles = [545, 543, 544]
        warnings.warn("We're forcing the bundles {} to have measure prevalence for the modeler".format(prev_bundles))
        df.loc[df.bundle_id.isin(prev_bundles), 'icg_measure'] = 'prev'
        df = df[['bundle_id', 'icg_measure']].drop_duplicates()
        df = df.drop_duplicates()
        assert df[df.bundle_id.duplicated(keep=False)].shape[0] == 0,\
            "Bundles have duped icg measures"
        df.rename(columns={'icg_measure': 'measure'}, inplace=True)
        df['measure'] = df['measure'].astype(str)

        stata_path = "FILEPATH"\
                     "FILEPATH".format(self.run_id)
        df.to_stata(stata_path, write_index=False)
        return

    def job_holder(self, job_name, sleep_time=65):
        """Checks the users qstat every n seconds to see if any ms jobs are
        still running

        Parameters
        ----------
        job_name : str
            Name of the job.
        sleep_time : int
            Time in seconds to sleep between qstat calls

        Returns
        -------
        None

        """
        print("..sleep..")
        time.sleep(60)

        status = "wait"
        while status == "wait":
            p = subprocess.Popen("qstat", stdout=subprocess.PIPE)

            qstat_txt = p.communicate()[0]
            qstat_txt = qstat_txt.decode('utf-8')

            job_count = qstat_txt.count(job_name)
            if job_count == 0:
                status = "go"
                print("Continue processing data")
            else:
                print("..keep sleeping..")
                time.sleep(sleep_time)
        return

    def rm_old_logs(self):
        """(In development) Removes old claims log files to prevent too many
        from accumulating.

        Returns
        -------
        None

        """

        return

    def pre_nr_agg(self):
        """Submits the `FILEPATH` job
        to the cluster with switches 1 0 0 0 0 1 and the current run id.

        Returns
        -------
        None

        """
        qs = 'qsub'.\
             format(repo=self.repo, rid=self.run_id)

        print(qs)
        subprocess.call(qs, shell=True)
        return

    def ms_nr(self, facilities):
        """Submits the `FILEPATH` script to
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

        qs = 'qsub'.\
             format(repo=self.repo, swi=switches, rid=self.run_id)

        print(qs)
        subprocess.call(qs, shell=True)

    def write_bundle_csv(self):
        """Submits the `FILEPATH` script to the

        Returns
        -------
        None

        """
        qsub = "qsub".\
            format(repo=self.repo, step=self.decomp_step, r=self.run_id)
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

        base_dir = ("FILEPATH"
                    "FILENAME".format(run_id))


        file_list = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]


        regex_matches = [re.findall(pattern=regex_pattern, string=f) for f in file_list]


        regex_matches = [f for f in regex_matches if len(f) > 0]


        regex_matches = [f[0] for f in regex_matches]


        regex_matches = [re.findall(pattern="\d{1,3}\.", string=f) for f in regex_matches]


        group_numbers = [int(f[0].replace(".", "")) for f in regex_matches]


        group_numbers = list(set(group_numbers))


        all_groups = list(range(0, 350))


        return set(all_groups) - set(group_numbers)

    def main(self, run_step=0):
        """ Main method that controls the flow claims flow.

        main runs all steps equal to and less than the passed in `run_step`

        Step 0: create a list of bundle IDs
        Step 1: run the long Marketscan process to create estimates and data for correction factors
        Step 2: aggregate data for NR
        Step 3: run inpatient NR
        Step 4: run union of inp and otp NR

        Examples:
        run_step = 2:
        Step 0, 1 and 2 will be run.

        Parameters
        ----------
        run_step : int
            The upper bound on steps to run.

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
            print("Sending out the master script to process MS data to the individual level")
            run_marketscan(self.run_id)
            self.job_holder(job_name="ms_group")

        if run_step < 3:
            print("Qsubbing the legacy Stata code to aggregate before noise reduction")
            self.pre_nr_agg()
            self.job_holder(job_name="pre_nr_")

        if run_step < 4:
            print("Qsubbing the legacy Stata code to run Noise Reduction on inpatient data")
            self.ms_nr("inp")
            self.job_holder(job_name="stata_submit")
            self.job_holder(job_name="ms_nr")

        if run_step < 5:
            print("Qsubbing the legacy Stata code to run Noise Reduction on U(inpatient, outpatient) data")


            self.ms_nr("all")
            self.job_holder(job_name="stata_submit")
            self.job_holder(job_name="ms_nr")

        print("The claims process has finished running. Inputs for the CFs should be ready and NR"\
              " estimates are available in FILEPATH"\
              format(self.run_id))
        print("Sending out the job to format the final bundle level estimates for upload.")
        warnings.warn("we're applying the asfr adjustment for maternal data. Maternal Bundles are hard coded and decomp step is hardcoded")
        self.decomp_step = 'step1'
        self.write_bundle_csv()
        return

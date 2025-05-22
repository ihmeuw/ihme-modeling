####################################################
# Run ST-GPR modeling stages
#
# Date: May 17, 2018
####################################################

import os
import sys
import time

# Imports and environment settings
from multiprocessing import Pool

import numpy as np
import pandas as pd

import db_queries
import stgpr_schema
from gbd import release

from stgpr.legacy.st_gpr import helpers as hlp
from stgpr.lib import constants

np.seterr(invalid="ignore")


# Prep
###############################################################################


# Create run class
class Model:
    def __init__(
        self,
        run_id,
        run_type,
        location_group,
        spacevar="location_id",
        timevar="year_id",
        agevar="age_group_id",
        sexvar="sex_id",
        datavar="data",
        holdout_num=1,
        nparallel=1,
        param_sets="0",
    ):

        self.run_id = run_id
        self.run_type = run_type
        self.spacevar = spacevar
        self.timevar = timevar
        self.agevar = agevar
        self.orig_agevar = "{}_orig".format(self.agevar)
        self.sexvar = sexvar
        self.datavar = datavar
        self.loc_group = location_group
        self.holdout_num = holdout_num
        self.nparallel = nparallel
        self.param_sets = hlp.separate_string_to_list(str(param_sets), int)

        # conditionals
        settings = stgpr_schema.get_settings()
        output_root = settings.output_root_format.format(stgpr_version_id=run_id)
        if self.nparallel > 1:
            self.parallel = 1
            self.outroot = "{dir}/st_temp_{ko}".format(dir=output_root, ko=self.holdout_num)
        else:
            self.parallel = 0
            self.outroot = "{dir}/output_{ko}.h5".format(dir=output_root, ko=self.holdout_num)

        # central keys
        self.ids = [self.spacevar, self.timevar, self.agevar, self.sexvar]

        self.intermediate_dir = os.path.dirname(
            "{root}/amp_nsv_temp/".format(root=output_root)
        )

    def enumerate_parameters(self):
        """Initialize all model parameters"""

        # pull parameters
        parameters = hlp.model_load(self.run_id, "parameters", param_set=None)
        parameters = parameters.where((pd.notnull(parameters)), None)

        # st necessities
        self.st_version = parameters["st_version"].iat[0]
        self.location_set_id = int(parameters["location_set_id"].iat[0])
        self.release_id = parameters["release_id"].iat[0]
        self.gbd_round_id = release.get_gbd_round_id_from_release(self.release_id)
        self.prediction_location_set_version = parameters[
            "prediction_location_set_version_id"
        ].iat[0]
        self.standard_location_set_version = parameters[
            "standard_location_set_version_id"
        ].iat[0]

        # prediction vars
        self.prediction_location_ids = hlp.get_parallelization_location_group(
            self.prediction_location_set_version,
            self.standard_location_set_version,
            self.nparallel,
            self.loc_group,
        )
        self.prediction_year_ids = parameters["prediction_year_ids"].iat[0]
        self.prediction_age_group_ids = self.df[self.orig_agevar].unique().tolist()
        self.prediction_sex_ids = self.df[self.sexvar].unique().tolist()

        # age stuff
        if parameters["st_custom_age_vector"].iat[0]:
            self.custom_age_vector = [
                float(x) for x in parameters["st_custom_age_vector"].iat[0].split(",")
            ]
        else:
            self.custom_age_vector = None

        self.n_levels = int(max(self.df.level))
        self.level_cols = ["level_{i}".format(i=i) for i in range(self.n_levels + 1)]

    def prep_indata(self):
        """Bring in prepped data, location hierarchy and stage1 linear model estimates
        and merge together for input into spacetime stage"""

        # Retrieve data
        self.data = hlp.model_load(self.run_id, "prepped", holdout=self.holdout_num)
        self.stage1 = hlp.model_load(self.run_id, "stage1", holdout=self.holdout_num)
        all_locations = hlp.model_load(self.run_id, "location_hierarchy").query(
            "level >= {}".format(constants.location.NATIONAL_LEVEL)
        )
        self.all_locations = all_locations["location_id"].tolist()

        # Merge
        df = pd.merge(self.stage1, self.data, how="outer", on=self.ids)

        # force stupid level columns to float
        lvlcols = [s for s in df.columns if "level_" in s]
        df[lvlcols] = df[lvlcols].astype(float)

        # Drop any data specified as 1 by ko_{holdout_num}
        df = hlp.drop_ko_values(df, self.holdout_num, datavar=self.datavar)

        # Create temporary age index
        df[self.orig_agevar] = df[self.agevar]
        df[self.agevar] = df.groupby(self.agevar).grouper.group_info[0]

        # Sort
        self.df = df.sort_values(by=self.ids)

    def get_age_map(self) -> None:
        """Create an age vector from either user-specified
        vector or the age midpoint of each age_group_id to be predicted over,
        to allow for more realistic distances between ages"""

        # pull age start and ends and rename
        # IMPORTANT: don't pass release_id into get_age_spans, otherwise get_age_spans will
        # limit to only age groups in the GBD release
        age_database = db_queries.get_age_spans().rename(
            columns={"age_group_years_start": "age_start", "age_group_years_end": "age_end"}
        )
        # filter to prediction ages and find midpoint of each age group
        age_database = (
            age_database[age_database.age_group_id.isin(self.prediction_age_group_ids)]
            .pipe(self._calculate_age_midpoint)
            .reset_index(drop=True)
        )

        if self.custom_age_vector:
            print("age vectoring")
            age_midpoints = self.custom_age_vector
        else:
            age_midpoints = age_database["age_midpoint"]

        self.age_map = dict(zip(age_database[self.agevar].index, age_midpoints))

    def get_location_sex_list(self):
        """Build a list to parse out spacetime jobs to different cores based on location and sex"""

        ls_list = self.df[[self.spacevar, self.sexvar]].drop_duplicates()
        if self.parallel == 1:
            ls_list = ls_list[(ls_list.location_id.isin(self.prediction_location_ids))]
        ls_list = ls_list.values
        self.ls_list = tuple(map(tuple, ls_list))

    def count_loc_max_country_years(self):
        """
        Calculate the maximum number of country-years of data among
        age groups for a given country-level location and sex being modeled.
        To actually understand it, best to look at pictures, but imagine
        a bunch of time-series plots facet-wrapped by age and sex.
        If you counted the datapoints for each of those time-series plots,
        then just took the biggest one for each sex, that would be the
        maximum number of country-years for a given location/sex.

        Inputs: Pandas dataframe with location, year, age, sex, and data
        Outputs: Pandas dataframe with unique location, sex, and
        maximum country-years values
        """

        cy = self.df.dropna(subset=[self.datavar])[self.ids + [self.datavar]]
        cy = cy.drop_duplicates(subset=self.ids)

        cy = cy.drop_duplicates(
            subset=[self.spacevar, self.timevar, self.agevar, self.sexvar]
        )
        cy = cy.groupby([self.spacevar, self.agevar, self.sexvar]).agg("count")[self.datavar]
        cy = cy.groupby([self.spacevar, self.sexvar]).agg("max")

        location_cy = cy.to_frame(name="location_id_count").reset_index()

        # make sure all locations in frame, even ones with zero cy of data
        comps = [self.all_locations, self.prediction_sex_ids]
        names = [self.spacevar, self.sexvar]
        loc_sex_df = hlp.square_skeleton(components=comps, names=names)
        location_cy = location_cy.merge(loc_sex_df, how="outer")
        location_cy = location_cy.fillna(0).sort_values(names)

        self.location_country_years = location_cy

    def ensure_loc_id_count_in_df(self):

        if "location_id_count" not in self.df.columns:
            self.df = self.df.merge(
                self.location_country_years, on=[self.spacevar, self.sexvar]
            )

    def get_hyperparameters(self, param_set=None):
        """Set up hyperparameters for ko-runs (ko), different-hyperparams-by-data-density-runs (dd)"""

        hlp.model_load(run_id, "parameters", holdout=holdout_num, param_set=param_set)
        store = pd.HDFStore(hlp.model_path(self.run_id, "parameters"), "r")
        self.hyperparams = store.get("parameters_{}".format(param_set))
        store.close()

    def assign_hyperparameters(self, param_set):

        hype = hlp.model_load(
            run_id, "parameters", holdout=self.holdout_num, param_set=param_set
        )
        in_df = self.df.copy()

        if self.run_type in ["in_sample_selection", "oos_selection"]:
            hype = pd.DataFrame(dict(zip(hype.keys(), hype.values)), index=[0])

        if self.run_type != "dd":
            hype["density_cat"] = 0
            in_df["density_cat"] = 0
        else:
            hype.sort_values(by="density_cutoffs", inplace=True)
            cutoffs = hype.density_cutoffs.tolist()
            hype["density_cat"] = list(range(0, len(cutoffs)))

            in_df = hlp.assign_density_cats(
                df=in_df, country_year_count_var="location_id_count", cutoffs=cutoffs
            )

        self.in_df = in_df.merge(hype, on="density_cat", how="left")

    def launch(self):
        p = Pool(processes=6)

        st = p.map(
            hlp.st_launch(self.in_df, self.run_id, self.age_map, self.st_version),
            self.ls_list,
        )

        p.close()

        # Concatenate list
        self.spacetime_estimates = pd.concat(st)

    def clean_st_results(self):

        self.spacetime_estimates[self.agevar] = self.spacetime_estimates[self.orig_agevar]
        spacetime_estimates = self.spacetime_estimates.drop(self.orig_agevar, 1)

        cols = self.ids + ["st", "scale"]
        self.spacetime = spacetime_estimates[cols]

    def save_st_results(self, param_set=None):

        # output
        if self.parallel == 1:

            param_set_outpath = "{}/{}".format(self.outroot, param_set)
            outpath = "{}/{}/{}.csv".format(self.outroot, param_set, self.loc_group)

            if not os.path.isdir(self.outroot):
                os.system("mkdir -m 777 -p {}".format(self.outroot))
            if not os.path.isdir(param_set_outpath):
                os.system("mkdir -m 777 -p {}".format(param_set_outpath))

            self.spacetime.to_csv(outpath, index=False)
        else:

            hlp.model_save(
                self.spacetime,
                self.run_id,
                "st",
                holdout=self.holdout_num,
                param_set=param_set,
                mode="w",
            )

    def _calculate_age_midpoint(self, age_database: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the age midpoint given columns 'age_start' and
        'age_end'. In specific age group cases where this doesn't make sense
        (ex: 80+), assign a midpoint.

        Note:
            Age smoothing relies on the assumption that age groups in
            age_database are organized in sort-order by age_group_id,
            ascending. As age group ids are mapped from their values (10, 11,
            12, 22, 388, ...)  to their indices (0, 1, 2, 3 ...), it is crucial
            midpoints are NOT sorted by age start/end and retain their original
            order instead.
        """
        age_database["age_midpoint"] = (
            age_database["age_start"] + age_database["age_end"]
        ) / 2

        specific_age_midpoints = constants.age_group.SPECIFIC_AGE_MIDPOINTS[self.gbd_round_id]

        for age_group_id, midpoint in specific_age_midpoints.items():
            age_database.loc[
                age_database[self.agevar] == age_group_id, "age_midpoint"
            ] = midpoint

        # divide by 5 to normalize for 5-year age groups
        age_database["age_midpoint"] = age_database["age_midpoint"] / 5

        return age_database


if __name__ == "__main__":
    run_id = int(sys.argv[1])
    holdout_num = int(sys.argv[2])
    run_type = sys.argv[3]
    param_sets = sys.argv[4]
    nparallel = int(sys.argv[5])
    if nparallel > 0:
        location_group = int(sys.argv[6])

    for i in [
        "run_id",
        "holdout_num",
        "run_type",
        "param_sets",
        "nparallel",
        "location_group",
    ]:
        print("{} : {}".format(i, eval(i)))

    start = time.time()

    m = Model(
        run_id=run_id,
        run_type=run_type,
        location_group=location_group,
        holdout_num=holdout_num,
        nparallel=nparallel,
        param_sets=param_sets,
        spacevar="location_id",
    )

    print("Prepping indata")
    m.prep_indata()

    print("Getting parameters")
    m.enumerate_parameters()

    print("Determining age vector")
    m.get_age_map()

    print("Preparing for multiprocessing")
    m.get_location_sex_list()

    for i in m.param_sets:

        # Get hyperparameters and add to dataset
        m.count_loc_max_country_years()
        m.ensure_loc_id_count_in_df()
        m.assign_hyperparameters(param_set=i)

        # Launch
        print(
            ("Running {} spacetime for param_set {}, " "locations {}").format(
                m.st_version, i, m.prediction_location_ids
            )
        )
        m.launch()

        # Clean
        print("Cleaning results")
        m.clean_st_results()

        # Output
        m.save_st_results(param_set=i)
        print("Spacetime outputs saved to {path}".format(path=m.outroot))

    end = time.time()
    print("Total time to completion: {sec} seconds".format(sec=end - start))

import pandas as pd
import sqlalchemy as sql
import numpy as np
import matplotlib.pyplot as plt
import dill
#import pylab
import os, sys
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.Ensemble import Ensemble
import codem.db_connect as db_connect
from debug_utils import get_submodels, get_acause
from run_all_codem import rerun_models

def pause():
    programPause = raw_input("Wait for the model to finish. \
                                Then press the <ENTER> key to continue...")

class DebugSubmodels:
    """

    """
    def __init__(self, model_version_id, db_connection='INSERT_DATABASE_NAME'):
        """

        """
        pickle_path = 'INSERT_PATH_WHERE_MODEL_IS_SAVED'\
                            .format(acause=get_acause(model_version_id,
                            db_connection), mvid=model_version_id)

        self.model = dill.load(open(pickle_path, "rb"))
        self.db_connection = db_connection

        ## all the predictions
        # higher level indicates type, lower level indicates stage
        self.preds = {}
        self.preds["lin"] = \
            {"lm": pd.DataFrame(self.model.linear_models.pred_mat_lin)}#,
#            "st": pd.DataFrame(self.model.linear_models.pred_mat_st),
#            "gpr": pd.DataFrame(self.model.linear_models.pred_mat_gpr)}
        self.preds["st"] = \
            {"lm": pd.DataFrame(self.model.st_models.pred_mat_lin),
            "st": pd.DataFrame(self.model.st_models.pred_mat_st),
            "gpr": pd.DataFrame(self.model.st_models.pred_mat_gpr)}

        # get the submodels
        all_subs = get_submodels(self.model.model_version_id, self.db_connection)
        all_subs.submodel_type_id = \
            all_subs.submodel_type_id.map(lambda x: "lin" if x ==1 else "st")
        self.subs = {"all": all_subs,
                    "st": all_subs.loc[all_subs.submodel_type_id == 2, :].reset_index(),
                    "lin": all_subs.loc[all_subs.submodel_type_id == 1, :].reset_index()}

        # input data
        self.df = self.model.mod_inputs.data_frame
        self.df["ln_rate"] = cf_to_ln_rate(self.df.cf, self.df)
        self.df["lt_cf"] = self.df["cf"].map(lambda x: np.log(x / (1 - x)))


    def submodels_by_rank(self, submod_type="all"):
        """

        """
        sorted_subs = self.subs[submod_type].sort_values("rank")
        return sorted_subs.submodel_version_id


    def plot_submodels_by_location(self, location_id, age_id, submod_type="st",
                                                stage="gpr", space="ln_rate"):
        """

        """
        # just get the specified location and age
        indices = self.df.loc[(self.df.location_id == location_id) & \
                        (self.df.age == age_id), :].sort_values("year").index
        # loop through submodels and plot
        for i in range(len(self.subs[submod_type])):
            submod_dep_i = self.subs[submod_type].ix[i, "submodel_dep_id"]
            if ((space == "ln_rate") & (submod_dep_i == 1)):
                preds_i = \
                    pd.Series(lt_cf_to_ln_rate(self.preds[submod_type][stage].\
                    ix[indices, i], self.df.ix[indices, :])).reset_index().ix[:, 1]
            elif ((space == "lt_cf") & (submod_dep_i == 2)):
                preds_i = \
                    pd.Series(ln_rate_to_lt_cf(self.preds[submod_type][stage].\
                    ix[indices, i], self.df.ix[indices, :])).reset_index().ix[:, 1]
            else:
                preds_i = pd.Series(self.preds[submod_type][stage].ix[indices, i]).\
                    reset_index().ix[:, 1]
            preds_i.plot()
        # plot data for comparison
        data = pd.Series(self.df.ix[indices, space]).reset_index().ix[:,1]
        plt.plot(range(len(self.df.ix[indices,:])), data, "or")
        plt.legend(bbox_to_anchor=(1.25, 1),
                labels=self.subs[submod_type].index)
        return


    def get_modeled_locations(self):
        """

        """
        loc_ids = tuple(self.df.location_id.unique())
        call = """
               SELECT location_name
               FROM shared.location
               WHERE location_id in {}
               """.format(loc_ids)
        loc_names = db_connect.query(call, self.db_connection).values.tolist()
        return pd.DataFrame({"id": loc_ids, "name": loc_names})



def compare_submodel_ranks(model_1, model_2):
    """

    :param model_1: DebugSubmodels object
        first model to look at the submodel ranks from
    :param model_2: DebugSubmodels object
        second model to look at the submodel ranks from. should be the same
        cause, etc. as model_1 for comparability
    :return: bool

    """
    order_1 = model_1.subs["all"].sort_values("rank").index
    order_2 = model_2.subs["all"].sort_values("rank").index
    print "Model 1 Submodel Order: ", order_1
    print "Model 2 Submodel Order: ", order_2
    return (order_1 == order_2).all()

###############################################################################
############################### helper functions ##############################
###############################################################################

def cf_to_ln_rate(cf, df):
    """

    """
    rate = cf.values * df["envelope"].values / df["pop"].values
    return np.log(rate)

def lt_cf_to_ln_rate(lt, df):
    """

    """
    cf = np.exp(lt)/(1 + np.exp(lt))
    return cf_to_ln_rate(cf, df)

def ln_rate_to_lt_cf(ln, df):
    """

    """
    rate = np.exp(ln)
    cf = rate.values * df["pop"].values / df["envelope"].values
    return logit(cf)

def logit(x):
    """

    """
    return np.log(x / (1 - x))

def compare_corresponding_submodels(list_of_models, location_id, age,
                                            submod_rank=1, stage="gpr"):
    """

    :param mod_1: DebugSubmodels object
        first model to look at the submodels from
    :param mod_2: DebugSubmodels object
        second model to look at the submodels from. should be the same cause,
        etc. as model_1 for comparability
    """
    for mod_i in list_of_models:
        type_i = mod_i.subs["all"].loc[mod_i.subs["all"]["rank"] == submod_rank,
                                    "submodel_type_id"]
        idx = mod_i.subs[type_i].loc[mod_i.subs[type_i]["rank"] == submod_rank,
                                                                    : ].index[0]
        pd.Series(mod_i.preds[type_i][stage].ix[:, idx]).plot()

import matplotlib as mpl
mpl.use("Agg")  # make sure matplotlib is working non-interactively. This switches it to a different backend.
import pandas as pd
import numpy as np 
from ml_crosswalk.labels import SharedLabels, get_save_dir
import math
from scipy.stats import pearsonr
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns
import os


def predict_data_two_step(topic, algorithm, version, estimand_data, first_estimator, second_estimator, estimand,
                          first_estimand, second_estimand, first_model, second_predictions):
    """ This function is essential in the "crosswalk" two-step modeling process. It takes the predictions generated from
    the second model and feeds them into the first model. It returns a dataFrame containing new predictions from the
    two-step model

    Parameters
    ----------
    topic :
        a string declaring the risk topic of research
    algorithm :
        a string declaring the type of machine-learning algorithm being used
    version :
        a string declaring the model version (used for saving)
    estimand_data :
        a dataFrame of the target data
    first_estimator :
        a string declaring the estimator used in the first model
    second_estimator :
        same as above, but for the second model
    estimand :
        a string declaring the thing being estimated
    first_target_col :
        a string declaring the target column used in the first model
    second_target_col :
        same as above, but for the second model
    first_model :
        the first machine-learning model used in the two-step process
    second_predictions :
        a dataFrame resulting from generating predictions in the second model

    Returns
    -------
    new_predictions :

    """

    save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=second_estimator, version=version)
    save_dir += 'through_' + first_estimator.lower() + '/'

    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    new_df = second_predictions.rename(columns={'{}_prediction'.format(algorithm): second_estimand})
    print(new_df.head())

    new_predictions = first_model.predict(new_df=new_df, save_dir=save_dir)
    if new_predictions.shape[0] == 0:
        raise ValueError("Oops! You have no predictions.")

    estimand_data = estimand_data.rename(columns={first_estimand: 'actual'})
    new_graphing_df = new_predictions.merge(estimand_data, how='inner', suffixes=['_predicted', '_actual'],
                                            on=['year_id', 'age_group_id', 'sex_id', 'super_region_id', 'me_name',
                                                'location_id'])

    labs = SharedLabels()
    sups = labs.super_regions()
    new_graphing_df = new_graphing_df.merge(sups)

    new_graphing_df = new_graphing_df.rename(columns={'{}_prediction'.format(algorithm): 'predicted'})
    print(new_graphing_df.head())

    first_model.estimator = second_estimator
    metrics_obj = Metrics(new_graphing_df, algorithm=algorithm, model=first_model, save_dir=save_dir)
    metrics_obj.get_indiv_metrics()
    metrics_obj.print_facet_plots()
    return new_predictions


def plot_predictions_vs_estimator_data(topic, version, sups, year_start, year_end, estimator,
                                       estimand, predictions, algorithm):
    """ Graphs predictions from the first model versus the original data. """

    to_graph = predictions.merge(sups)
    to_graph = to_graph.sort_values(['me_name','super_region_id'])

    estimator_col = estimator.lower() + '_data'
    if len(to_graph.me_name.unique()) == 1:
        num_cols = 1
    elif len(to_graph.me_name.unique()) == 2:
        num_cols = 2
    else:
        num_cols = 3

    g = sns.FacetGrid(to_graph[(to_graph.year_id >= year_start) & (to_graph.year_id <= year_end)], col='me_name',
                      hue='super_region_name', col_wrap=num_cols, size=5, sharex=False, sharey=False)

    g = g.map(plt.scatter, '{}'.format(estimator_col), '{}_prediction'.format(algorithm),
              marker='o', alpha=0.5, facecolors='none', linewidth=1.5)
    g.add_legend(title='Super Region')
    g.set_axis_labels("{} mean".format(estimator), "{} prediction".format(estimand))
    g.fig.subplots_adjust(top=.85)  # previously .9
    g.fig.suptitle('{} Predictions vs {} Mean\nYear Id >={} and <= {}\nUsing {}: Version {}'
                   .format(estimand, estimator, year_start, year_end,
                                                 algorithm.upper(), version), fontsize=16)
    if num_cols == 1:
        for ax in g.axes.flat:
            plt.setp(ax.texts, text='')

    save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=None, version=version)
    file_path = save_dir + '{}_original_{}_predictions_vs_{}_data_{}_to_{}.png'.format(algorithm, estimand, estimator,
                                                                                  year_start, year_end)
    print("\n\n")
    print('Saving figure to {}'.format(file_path))
    plt.savefig(file_path)
    # plt.show()
    plt.clf()
    return


def make_st_gpr_plots(topic, version, sups, estimand, loc_id, me, total, algorithm):
    """ Graphs all predictions (including original data) in ST-GPR style. """

    to_graph = total.merge(sups).sort_values('me_name')
    to_graph = to_graph[['age_group_id', 'location_id', 'super_region_id', 'sex_id', 'me_name',
                         'year_id', '{}_data'.format(estimand.lower()), 'estimator']]

    # Select a few countries for which to make st-gpr plots
    to_graph = to_graph[(to_graph.location_id == loc_id) & (to_graph.me_name == me)]

    # Not every country has all modelable entities
    if len(to_graph) == 0:
        return  

    to_graph = to_graph[~(to_graph.age_group_id == 21) & (to_graph.age_group_id <= 30)]
    if not to_graph.empty:

        plt.figure()
        g = sns.FacetGrid(to_graph, col='age_group_id', hue='estimator', col_wrap=4, size=5, sharex=False, sharey=False)
        g = g.map(plt.scatter, 'year_id', '{}_data'.format(estimand.lower()),
                  marker='o', alpha=0.5, facecolors='none', linewidth=1.5)
        g.add_legend(title='Data Type')
        g.fig.subplots_adjust(top=.85)  # previously .9
        g.fig.suptitle('Observed and Estimated {} Data Time Trends\nfor {}\nin Location ID {}\n Using {}, Version {}'
                       .format(estimand, me, loc_id, algorithm.upper(), version), fontsize=16)


        save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=None, version=version) + \
            'st_gpr_plots/' + algorithm + '/'

        if not os.path.exists(save_dir):
            try:
                os.makedirs(save_dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        file_path = save_dir + 'location_{}_me_{}.png'.format(loc_id, me)
        print("\n\n")
        print('Saving figure to {}'.format(file_path))
        plt.savefig(file_path)
        plt.show()
        plt.clf()
    else:
        print("Your graphing dataframe is empty!")

    return


class Metrics(object):
    def __init__(self, graphing_df, save_dir, model, algorithm):
        self.graphing_df = graphing_df
        self.model = model
        self.save_dir = save_dir 
        self.algorithm = algorithm
        self.metrics_df = None

    def get_indiv_metrics(self):
        """ """
        rmse = {}
        peard = {}

        me_names = self.graphing_df.me_name.unique()
        print(self.graphing_df.head())
        print(self.graphing_df.columns)
        for i in range(0, len(me_names)):
            sub_dat = self.graphing_df[self.graphing_df.me_name == me_names[i]]
    
            mse = mean_squared_error(sub_dat.actual.values, sub_dat.predicted.values)
            pearson = pearsonr(sub_dat.actual.values, sub_dat.predicted.values)

            rmse[me_names[i]] = math.sqrt(mse)
            peard[me_names[i]] = str(pearson[0])

        rd = pd.Series(rmse, name='rmse')
        rd.index.name = 'me_name'
        rd = rd.reset_index()

        pcd = pd.Series(peard, name='pearson_corr')
        pcd.index.name = 'me_name'
        pcd = pcd.reset_index()

        metrics = rd.merge(pcd)
        file_path = self.save_dir + '{}_me_name_metrics.csv'.format(self.algorithm)
        print("\n\n")
        print('Saving metrics to {}'.format(file_path))
        print(metrics.sort_values('pearson_corr', ascending=False))
        metrics.to_csv(file_path)
        self.metrics_df = metrics

        return

    def print_facet_plots(self):
        """ """
        scat_dat = self.graphing_df

        # In case levels were dropped
        scat_dat['me_name'] = scat_dat['me_name'].astype('str')
        scat_dat['me_name'] = scat_dat['me_name'].astype('category')

        print(scat_dat.describe())

        maxes = scat_dat[['me_name', 'actual']].groupby('me_name').max().reset_index()

        maxes = maxes.rename(columns={'actual': 'predicted'})
        maxes = maxes.append(scat_dat[['me_name', 'predicted']].\
                                       groupby('me_name').max().reset_index())
        maxes = maxes.rename(columns={'predicted': 'value'})
        maxes = maxes[['me_name', 'value']].groupby('me_name').max().reset_index()
        print(maxes)

        if len(scat_dat.me_name.unique()) == 1:
            num_cols = 1
        elif len(scat_dat.me_name.unique()) == 2:
            num_cols = 2 
        else:
            num_cols = 3
        g = sns.FacetGrid(scat_dat, col='me_name', hue='super_region_name', col_wrap=num_cols, size=5,
                          sharex=False, sharey=False)

        g = g.map(plt.scatter, 'actual', 'predicted', marker='o', alpha=0.5, facecolors='none', linewidth=1.5)


        sorted_mets = self.metrics_df.sort_values('me_name')
        sorted_mets['pearson_corr'] = sorted_mets['pearson_corr'].astype('float')
        sorted_mets['pearson_corr'] = np.round(sorted_mets['pearson_corr'], decimals=3)
        sorted_mets['rmse'] = np.round(sorted_mets['rmse'], decimals=3)

        sorted_mets['metric_text'] = 'Pearson corr: ' + sorted_mets['pearson_corr'].map(str) + '; RMSE: ' + \
            sorted_mets['rmse'].map(str)
        metric_text = sorted_mets['metric_text'].tolist()

        g.add_legend(title='Super Region')
        g.set_axis_labels("Actual mean", "Predicted mean")
        g.fig.subplots_adjust(top=.85)  # previously .9
        g.fig.suptitle('{} Predictions vs Actual \nUsing {} and {} Data'
                       .format(self.model.estimand, self.algorithm.upper(), self.model.estimator), fontsize=16)

        custom_max = []
        # See https://stackoverflow.com/questions/41511334/adding-text-to-each-subplot-in-seaborn
        for i, ax in enumerate(g.axes.flat): # set every-other axis for testing purposes
            # if i == 0:#[enter link description here][1]:
            custom_max.append(maxes.iloc[i][1]*1.05)
            ax.set_xlim(0, custom_max[i])
            ax.set_ylim(0, custom_max[i])
            ax.text(custom_max[i]/2, custom_max[i]*0.975, '{}'.format(str(metric_text[i])), ha='center', fontsize=12)

        for ax, me_name in zip(g.axes.flat, metric_text):
            ax.plot((0, maxes.value.max()), (0, maxes.value.max()), c=".2", ls="-")

        g.set_titles(row_template='1', col_template='{col_name}')

        file_path = self.save_dir + '{}_pred_vs_actual.png'.format(self.algorithm)
        print("\n\n")
        print('Saving figure to {}'.format(file_path))
        plt.savefig(file_path)
        plt.show()
        plt.clf()

        return

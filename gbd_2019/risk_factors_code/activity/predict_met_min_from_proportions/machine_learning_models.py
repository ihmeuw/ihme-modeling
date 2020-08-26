import matplotlib as mpl
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error
from scipy.stats import pearsonr
import xgboost
from xgboost import plot_importance
import pickle
from pathlib import Path


def print_feature_to_target_correlations(df, target, features, save_dir):
    """
    Prints correlations between the features and the target column.

    :param df: a Pandas DataFrame on which you wish to print correlations
    :param target: a Pandas Series of the target values of a machine learning model
    :param features: a Pandas DataFrame of the features of a machine learning model
    :param save_dir: a directory to save the correlations.
    """
    correlations = {}
    target_name = target.iloc[:, 0].name
    for f in features.columns.tolist():
        data_temp = df[[f, target_name]]
        x1 = data_temp[f].values
        x2 = data_temp[target_name].values
        key = f + ' vs ' + target_name

        correlations[key] = pearsonr(x1, x2)[0]
        data_correlations = pd.DataFrame(correlations, index=['Pearson correlation']).T
        data_correlations = data_correlations.loc[data_correlations['Pearson correlation']
                                                  .abs().sort_values(ascending=False).index]

        print(data_correlations)
        file_path = save_dir + 'feature_vs_target_data_correlations.csv'
        print('Saving correlations to {}'.format(file_path))
        data_correlations.to_csv(file_path)


class MachineLearningModel(object):
    """
    This class utilizes the sklearn API to build an abstract machine learning model base class that can be
    extended indefinitely as more models are added to the hybrid model.
    """
    def __init__(self, oh_matched_data, estimand, estimator, save_dir):
        self.oh_matched_data = oh_matched_data
        self.estimand = estimand
        self.estimator = estimator
        self.save_dir = save_dir
        self.features = None
        self.target = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.predicted_test = None
        self.predicted_train = None
        self.model = None
        self.algorithm = None

    def train(self, feature_cols, target_col):
        """
        Splits the data and trains the machine learning algorithm. Updates the relevant data fields. Generates
        metrics including Pearson correlation and OOS error.

        :param feature_cols: an array of strings denoting which columns are feature columns
        :param target_col: a string denoting which column is the target column
        """

        self.features = self.oh_matched_data[feature_cols]
        self.target = self.oh_matched_data[target_col.lower() + '_data'].values

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.features,
                                                                                self.target,
                                                                                train_size=0.7, test_size=0.3,
                                                                                random_state=42)
        # Check if NaNs are present
        for column in self.X_train.columns:
            if self.X_train.isnull()[column].any():
                raise ValueError('The column `{}` contains missing values.'.format(column))

    def fit_and_save(self, model):
        """
        Fits a model to the X and y training data. Predicts on the testing data. Saves the model to a directory,
        using a sequence of random integer indefinitely to append to the file name if a previous model already exists
        in the directory.ß

        :param model: the machine learning model
        """
        print("Now training the {} model. The shape of the training features and target are: ".format(self.algorithm))
        print(self.X_train.shape)
        print(self.y_train.shape)

        if 'category' in self.X_train.dtypes:
            raise ValueError("Categories are not recognized dtypes in at least one machine learning model. Please use "
                             "one-hot encoding instead.")

        model.fit(self.X_train, self.y_train)

        self.predicted_test = model.predict(self.X_test)
        self.model = model

        file_path = self.save_dir + '{}_model'.format(self.algorithm)
        while Path(file_path + '.sav').exists():
            file_path += str(np.random.randint(10))

    def predict(self, new_df, save_dir, unseen=None, draw_number=None):
        """
        Generates predictions for a new data set.

        :param new_df: a data frame on which to generate predictions
        :param save_dir: a directory to save the predictions as a CSV
        :param unseen: Unseen means never introduced into the program previously on this run.
        :param draw_number: an integer used for parallelizing computation across draws.
        :return: a Pandas DataFrame of the new predictions.
        """
        # Now that we have OOS correlation, re-train the model on ALL values

        new_features = new_df[self.features.columns]

        # this '.predict' below comes from scikit-learn
        new_predictions = self.model.predict(new_features)

        new_df['{}_prediction'.format(self.algorithm)] = new_predictions

        if not unseen:
            file_path = save_dir + '{}_predictions.csv'.format(self.algorithm)
            print('Printing predictions to {}\n\n'.format(file_path))

        else:
            file_path = save_dir + '{}_unseen_predictions_draw{}.csv'.format(self.algorithm, draw_number)
            print('Printing unseen predictions to {}\n\n'.format(file_path))

        print(new_df.head())
        new_df.to_csv(file_path)
        return new_df

    def print_feature_importances(self, save_dir):
        """
        Plots and prints the feature importances of a machine learning model.

        :param save_dir: a directory where the plot will be saved
        """
        plt.title('Feature Importances \n for Predicting {} using {} Data'.format(self.estimand, self.estimator))
        plt.xlabel('Relative Importance')
        file_path = save_dir + '{}_feature_importances.png'.format(self.algorithm)

        print('Saving figure to {}'.format(file_path))
        print('\n\n')
        plt.savefig(file_path,  bbox_inches='tight')
        plt.show()


class RandomForestModel(MachineLearningModel):
    def __init__(self, oh_matched_data, estimand, estimator, save_dir):
        super().__init__(oh_matched_data, estimand, estimator, save_dir)
        self.algorithm = 'rf'

    def train(self, feature_cols, target_col):
        """
        Trains model and generates relevant metrics including Pearson correlation and OOS error

        :param feature_cols: an array of strings of the columns denoting which are feature columns in the machine
                             learning model
        :param target_col:
        :return: a scikit-learn API compliant model
        """
        super().train(feature_cols, target_col.lower())

        model = RandomForestRegressor(n_estimators=500, max_depth=20, oob_score=True, random_state=0)
        super().fit_and_save(model)

        return self.model

    def print_importance_chart(self, save_dir):
        """
        Plots and prints the feature importances in a random forest model.

        :param save_dir: the directory in which to save the plot
        """
        importances = self.model.feature_importances_
        indices = np.argsort(importances)
        plt.figure(1)

        plt.barh(range(len(indices)), importances[indices], color='b', align='center')
        plt.yticks(range(len(indices)), self.features.columns[indices])

        super().print_feature_importances(save_dir)


class XGBoostModel(MachineLearningModel):
    def __init__(self, oh_matched_data, estimand, estimator, save_dir):
        super().__init__(oh_matched_data, estimand, estimator, save_dir)
        self.algorithm = 'xgb'

    def train(self, feature_cols, target_col):
        """
        Trains model and generates relevant metrics including Pearson correlation and OOS error

        :param feature_cols: an array of strings denoting the feature columns in the data frame
        :param target_col: a string denoting the target column in the machine learning model

         """
        super().train(feature_cols, target_col)

        model = xgboost.XGBRegressor(n_estimators=100, learning_rate=0.08, gamma=0, subsample=0.75,
                                     colsample_bytree=1, max_depth=7, random_state=42)
        super().fit_and_save(model)

        return self.model

    def tune_train(self, feature_cols, target_col):
        # Load data into DMatrices
        dtrain = xgboost.DMatrix(self.X_train, label=self.y_train)
        dtest = xgboost.DMatrix(self.X_test, label=self.y_test)

        # Build a baseline model
        # "Learn" the mean from the training data
        mean_train = np.mean(self.y_train)

        # get predictions on the test set
        baseline_predictions = np.ones(self.y_test.shape) * mean_train

        # Compute MAE
        mae_baseline = mean_absolute_error(self.y_test, baseline_predictions)
        print("Baseline MAE is {:.2f}".format(mae_baseline))

        params = {
            # Parameters that we are going to tune.
            'max_depth': 6,
            'min_child_weight': 1,
            'eta': .3,
            'subsample': 1,
            'colsample_bytree': 1,
            # Other parameters
            'objective': 'reg:linear'
        }

        params['eval_metric'] = 'mae'
        num_boost_round = 999

        model = xgboost.train(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            evals=[(dtest, "Test")],
            early_stopping_rounds=10
        )

        print("Best MAE: {:.2f} with {} rounds".format(
            model.best_score,
            model.best_iteration + 1))


        cv_results = xgboost.cv(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            seed=42,
            nfold=5,
            metrics={'mae'},
            early_stopping_rounds=10
        )

        print(cv_results)
        print(cv_results['test-mae-mean'].min())

        gridsearch_params = [
            (max_depth, min_child_weight)
            for max_depth in range(9, 12)
            for min_child_weight in range(5, 8)
        ]

        # Define initial best params and MAE
        min_mae = float("Inf")
        best_params = None
        for max_depth, min_child_weight in gridsearch_params:
            print("CV with max_depth={}, min_child_weight={}".format(
                max_depth,
                min_child_weight))

            # Update our parameters
            params['max_depth'] = max_depth
            params['min_child_weight'] = min_child_weight

            # Run CV
            cv_results = xgboost.cv(
                params,
                dtrain,
                num_boost_round=num_boost_round,
                seed=42,
                nfold=5,
                metrics={'mae'},
                early_stopping_rounds=10
            )

            # Update best MAE
            mean_mae = cv_results['test-mae-mean'].min()
            boost_rounds = cv_results['test-mae-mean'].argmin()
            print("\tMAE {} for {} rounds".format(mean_mae, boost_rounds))
            if mean_mae < min_mae:
                min_mae = mean_mae
                best_params = (max_depth, min_child_weight)

        print("Best params: {}, {}, MAE: {}".format(best_params[0], best_params[1], min_mae))
        params['max_depth'] = best_params[0]
        params['min_child_weight'] = best_params[1]

        gridsearch_params = [
            (subsample, colsample)
            for subsample in [i / 10. for i in range(7, 11)]
            for colsample in [i / 10. for i in range(7, 11)]
        ]

        min_mae = float("Inf")
        best_params = None

        # We start by the largest values and go down to the smallest
        for subsample, colsample in reversed(gridsearch_params):
            print("CV with subsample={}, colsample={}".format(
                subsample,
                colsample))

            # We update our parameters
            params['subsample'] = subsample
            params['colsample_bytree'] = colsample

            # Run CV
            cv_results = xgboost.cv(
                params,
                dtrain,
                num_boost_round=num_boost_round,
                seed=42,
                nfold=5,
                metrics={'mae'},
                early_stopping_rounds=10
            )

            # Update best score
            mean_mae = cv_results['test-mae-mean'].min()
            boost_rounds = cv_results['test-mae-mean'].argmin()
            print("\tMAE {} for {} rounds".format(mean_mae, boost_rounds))
            if mean_mae < min_mae:
                min_mae = mean_mae
                best_params = (subsample, colsample)

        print("Best params: {}, {}, MAE: {}".format(best_params[0], best_params[1], min_mae))

        params['subsample'] = best_params[0]
        params['colsample_bytree'] = best_params[1]

        # This can take some time…
        min_mae = float("Inf")
        best_params = None

        for eta in [.3, .2, .1, .05, .01, .005]:
            print("CV with eta={}".format(eta))

            # We update our parameters
            params['eta'] = eta

            # Run and time CV
            cv_results = xgboost .cv(
                params,
                dtrain,
                num_boost_round=num_boost_round,
                seed=42,
                nfold=5,
                metrics=['mae'],
                early_stopping_rounds=10
            )

            # Update best score
            mean_mae = cv_results['test-mae-mean'].min()
            boost_rounds = cv_results['test-mae-mean'].argmin()
            print("\tMAE {} for {} rounds\n".format(mean_mae, boost_rounds))
            if mean_mae < min_mae:
                min_mae = mean_mae
                best_params = eta

        print("Best params: {}, MAE: {}".format(best_params, min_mae))

        params['eta'] = best_params

        model = xgboost.train(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            evals=[(dtest, "Test")],
            early_stopping_rounds=10
        )
        print("Best MAE: {:.2f} in {} rounds".format(model.best_score, model.best_iteration + 1))

        num_boost_round = model.best_iteration + 1

        best_model = xgboost.train(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            evals=[(dtest, "Test")]
        )
        mean_absolute_error(best_model.predict(dtest), self.y_test)
        best_model.save_model("my_model.model")

    def print_importance_chart(self, save_dir):
        plot_importance(self.model)

        super().print_feature_importances(save_dir)


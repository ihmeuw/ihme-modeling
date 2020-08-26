from pathlib import Path


def _validate_config_file():
    config_file = Path('./config.ini')
    try:
        config_file.resolve()
    except FileNotFoundError:
        print('`config.ini` not found. Did you launch the script from the same directory as the `config.ini` file?')


def _validate_preserved_cols(preserved_cols, feature_cols):
    """ This test ensures there are no preserved columns in the feature columns argument. """

    for col in preserved_cols:
        for f_col in feature_cols:
            if col == f_col:
                raise ValueError("Preserved columns must not be included in feature columns. \n"
                                 "You have the column `{}` present in both arrays.".format(col))


def _validate_algorithm(algorithm):
    """ Validates the machine learning algorithm type argument based on what's been implemented in the code. """

    if (algorithm.lower() != 'rf') & (algorithm.lower() != 'xgb'):
        raise ValueError("Algorithm {} is not supported. Please choose from one of the following: \n"
                         "rf  : random forest \n"
                         "xgb : XGBoost".format(algorithm))


def _validate_matched_dataset(matched_df, estimand, estimator):
    """ """

    if matched_df.empty:
        raise ValueError('Your matched data set is empty! You tried matching {} and {} but received no results'
                         .format(estimand, estimator))


def _validate_input_columns(raw, required_columns):
    """ """

    for col in required_columns:
        if col not in raw.columns:
            raise ValueError("Your input data file is incomplete! You failed to provide the {} column.".format(col))

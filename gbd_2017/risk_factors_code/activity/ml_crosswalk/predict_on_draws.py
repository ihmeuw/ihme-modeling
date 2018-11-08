import pandas as pd
import sys
import re
from ml_crosswalk.labels import get_save_dir


if __name__ == "__main__":
    print("Message received")
    args = sys.argv[1:]
    raw = pd.read_csv(args[0])
    topic = args[1]
    estimator = args[2]
    estimand = args[3]
    estimator_object = args[4]
    ids = raw.filter(regex='.*[a-zA-Z]$')

    ##
    total = pd.DataFrame()
    for i in range(0, 1000):
        one_draw = raw.filter(regex='_{}$'.format(i))
        old_column_names = one_draw.columns.tolist()
        one_draw = one_draw.rename(columns=lambda x: re.sub('_[0-9]+$', '', x))
        new_column_names = one_draw.columns.tolist()

        one_draw = pd.concat([ids, one_draw], axis=1)
        one_draw['estimator'] = None
        one_draw['estimator'][one_draw.gpaq == 1] = 'gpaq'  # TODO:remove this hard-coding for cross-topic compatibility
        one_draw['estimator'][one_draw.gpaq != 1] = 'ipaq'
        one_draw['me_name'] = topic

        model = estimator_object[i]['model']
        matched_second_data = estimator_object['matched']

        save_dir = get_save_dir(topic, estimand, estimator)
        predictions = model.predict(new_df=matched_second_data, save_dir=save_dir, unseen=False)

        one_draw = one_draw[new_column_names]
        one_draw = one_draw.rename(columns=lambda x: x + '_{}'.format(i))
        completed = pd.concat([ids, one_draw], axis=1)

        if total.shape[0] == 0:
            total = completed
        else:
            total = total.merge(completed)

    total.to_csv(args[0][:-4] + '_predicted.csv')

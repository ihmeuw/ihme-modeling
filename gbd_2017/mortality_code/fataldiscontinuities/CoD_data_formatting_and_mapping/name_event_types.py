'''
Merge event_type_names onto the shocks db for human readability.
Contains simple tests to make sure passed file has the right columns
and shape.
'''
import pandas as pd


def name_event_types(db, event_type_name_path):


    names = pd.read_csv(event_type_name_path, encoding='latin1')
    desired_cols = ['event_type', 'event_type_name']
    if set(names.columns.tolist()) != set(desired_cols):
        raise ValueError('{path} must point to a file containing '
                         'only the columns {name1} and '
                         '{name2}'.format(path=event_type_name_path,
                                          name1=desired_cols[0],
                                          name2=desired_cols[1])
                         )
    if names.isnull().any().any():
        raise ValueError('null values not allowed '
                         'in {path}'.format(path=event_type_name_path))

    db = pd.merge(db, names, how='left', on='event_type')

    return db

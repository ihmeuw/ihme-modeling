import pandas as pd


def get_eligible_age_group_ids(age_start, age_end):
    """ Returns list of eligible age_group_ids between the age_start
        and age_end
    """
    # Age dictionaries and lists
    age_data = ['0', '0.01', '0.1', '1', '5', '10', '15', '20', '25', '30',
                '35', '40', '45', '50', '55', '60', '65', '70', '75', '80',
                '85', '90', '95']
    age_group_ids = {'10': 7, '60': 17, '30': 11, '15': 8, '25': 10, '0.1': 4,
                     '55': 16, '0.01': 3, '45': 14, '35': 12, '50': 15, '1': 5,
                     '0': 2, '75': 20, '5': 6, '40': 13, '70': 19, '20': 9,
                     '65': 18, '80': 30, '85': 31, '90': 32, '95': 235}
    # Make sure age_start and age_end are strings
    age_start = str(age_start)
    age_end = str(age_end)
    # Loop through and generate eligible age_groups
    return_list = []
    add = False
    for age in age_data:
        if age_start == age:
            add = True
        if add:
            return_list.append(age_group_ids[age])
        if age_end == age:
            add = False
    return return_list


def get_eligible_sex_ids(male, female):
    """ Returns list of eligible sex_ids based on input values
        for male and female
    """
    return_list = []
    if int(male):
        return_list.append(1)
    if int(female):
        return_list.append(2)
    return return_list


def expand_id_set(input_data, eligible_ids, id_name):
    """ Duplicates input data by the number of items in eligible ids and creates
        new column
        Returns: newly expanded DataFrame
    """
    result_df = []
    for i in eligible_ids:
        temp = input_data.copy(deep=True)
        temp[id_name] = i
        result_df.append(temp)
    return pd.concat(result_df)

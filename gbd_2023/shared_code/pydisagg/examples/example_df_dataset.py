import numpy as np
import pandas as pd

groups_to_split_into = [
    "age_group_0",
    "age_group_1",
    "age_group_2",
    "age_group_3",
]
baseline_patterns = pd.DataFrame.from_dict(
    {0: np.array([0.4, 0.2, 0.3, 0.75]), 1: np.array([0.1, 0.15, 0.2, 0.7])},
    orient="index",
)
baseline_patterns.index.name = "pattern_id"
baseline_patterns.columns = groups_to_split_into


# Population ids correspond to the overall aggregated population we are splitting
# For example, this would be the id for a country

pop_group_sizes = {
    0: [10, 10, 20, 20],
    1: [10, 20, 20, 13],
    2: [20, 20, 10, 10],
    3: [5, 40, 30, 20],
}
population_sizes = pd.DataFrame.from_dict(
    pop_group_sizes, orient="index", columns=groups_to_split_into
)
population_sizes.index.name = "demographic_id"

pop_df = pd.DataFrame(
    {"demographic_id": [0, 1, 2, 3], "pattern_id": [0, 0, 1, 1]}
)

group_partitions = {
    0: [("age_group_0", "age_group_1"), ("age_group_2", "age_group_3")],
    1: [("age_group_0", "age_group_1", "age_group_2"), ("age_group_3",)],
    2: [("age_group_0", "age_group_1", "age_group_2", "age_group_3")],
    3: [("age_group_0"), ("age_group_1"), ("age_group_2", "age_group_3")],
}


def get_dummies(partition, splitting_groups):
    dummies = [
        [int(group in data_partition) for group in splitting_groups]
        for data_partition in partition
    ]
    return dummies


def build_dummy_df(dummies, id, groups_to_split_into):
    df = pd.DataFrame(dummies, columns=groups_to_split_into)
    df["demographic_id"] = id
    return df


splitting_df = pd.concat(
    [
        build_dummy_df(
            get_dummies(partition, groups_to_split_into),
            id,
            groups_to_split_into,
        )
        for id, partition in group_partitions.items()
    ]
)

observations = [7, 20, 10, 11, 10, 0.8, 8, 22]

SE_vals = [1, 2, 3, 1, 1.5, 0.1, 1, 3]

data_df = pop_df.merge(splitting_df, on="demographic_id")

data_df["obs"] = observations
data_df["obs_se"] = SE_vals

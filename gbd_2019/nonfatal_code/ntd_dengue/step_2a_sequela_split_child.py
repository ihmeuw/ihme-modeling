import numpy as np
import pandas as pd
import argparse
import os
from ADDRESS import get_draws

### Parse qsub args
parser = argparse.ArgumentParser()
parser.add_argument("--params_dir", help="Directory containing params", type=str)
parser.add_argument("--draws_dir", help="Directory containing draws", type=str)
parser.add_argument("--interms_dir", help="Directory containing interms", type=str)
parser.add_argument("--logs_dir", help="Directory containing logs", type=str)
parser.add_argument("--location_id", help="Location id to run for", type=str)
args = vars(parser.parse_args())

params_dir = args["params_dir"]
draws_dir = args["draws_dir"]
interms_dir = args["interms_dir"]
logs_dir = args["logs_dir"]
location_id = args["location_id"]

draws_dir = "FILEPATH"

### Script Logic
df = pd.read_csv(f"FILEPATH")

# Create vector n=1000 of beta distribution
def custom_beta(u, s):
    a = u * (u - u**2 - s**2) / s**2
    b = a * (1 - u) / u
    dist = np.random.beta(a, b, (1000))
    return dist

# Apply each meid params
p_ADDRESS1 = custom_beta(u=0.945, s=0.074)
p_ADDRESS2 = custom_beta(u=0.055, s=0.00765)
p_ADDRESS3 = custom_beta(u=0.084, s=0.02)

def apply_prob_correction(p_ADDRESS1, p_ADDRESS2):
    correction = p_ADDRESS1 + p_ADDRESS2
    p_ADDRESS1 = p_ADDRESS1 / correction
    p_ADDRESS2 = p_ADDRESS2 / correction
    return (p_ADDRESS1, p_ADDRESS2)

p_ADDRESS1, p_ADDRESS2 = apply_prob_correction(p_ADDRESS1, p_ADDRESS2)

def apply_prob(df, prob):
    df = df.copy()
    draw_cols = df.columns[df.columns.str.contains("draw")]
    df[draw_cols] = df[draw_cols].apply(lambda x: x * prob, axis=1)
    return df

# Apply probabilities to base draws
df_ADDRESS1 = apply_prob(df, p_ADDRESS1)
df_ADDRESS2 = apply_prob(df, p_ADDRESS2)
df_ADDRESS3 = apply_prob(df, p_ADDRESS3)

def apply_duration(df, duration):
    prev = df.copy()
    draw_cols = df.columns[df.columns.str.contains("draw")]
    prev[draw_cols] = prev[draw_cols] * duration
    prev["measure_id"] = 5
    df = df.append(prev)
    return df

# Adjust incidence to prevalence w/ durations
# Source of durations: Whitehead et al, doi: 10.1038/nrmicro1690
df_ADDRESS1 = apply_duration(df_ADDRESS1, 6/365)
df_ADDRESS2 = apply_duration(df_ADDRESS2, 14/365)
df_ADDRESS3 = apply_duration(df_ADDRESS3, 0.5)
df_ADDRESS1["modelable_entity_id"] = ADDRESS
df_ADDRESS2["modelable_entity_id"] = ADDRESS
df_ADDRESS3["modelable_entity_id"] = ADDRESS


# Saves (ADDRESS3 is prevalence only)
df_ADDRESS1.to_csv(f"FILEPATH/FILEPATH", index=False)
df_ADDRESS2.to_csv(f"FILEPATH//FILEPATH", index=False)
df_ADDRESS3[df_ADDRESS3.measure_id==5].to_csv(f"FILEPATH/FILEPATH", index=False)

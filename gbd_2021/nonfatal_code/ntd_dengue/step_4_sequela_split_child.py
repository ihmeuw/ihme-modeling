#NTDs Dengue
#Description: apply proportional sequela splits based on coeffs and apply duration for prevalence, save NF meids "ADDRESS1"-"ADDRESS3"

### ======================= BOILERPLATE ======================= ###
import os
import argparse
import numpy as np
import pandas as pd
# import db_queries as db
from "FILEPATH" import get_draws

code_root = "FILEPATH"
data_root = "FILEPATH"
cause = "ntd_dengue"

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"
location_id = 4715

### Define Constants
gbd_round_id = "ADDRESS"
decomp_step = "ADDRESS"

### Script Logic
df = get_draws("model_id", "ADDRESS", "ADDRESS", location_id=location_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step, version_id="ADDRESS")
df = df.drop(["version_id"], axis=1)
df["metric_id"] = 3

# Create vector n=1000 of beta distribution
def custom_beta(u, s):
    a = u * (u - u**2 - s**2) / s**2
    b = a * (1 - u) / u
    dist = np.random.beta(a, b, (1000))
    return dist

# Apply each meid params
p_"ADDRESS1" = custom_beta(u=0.945, s=0.074)
p_"ADDRESS2" = custom_beta(u=0.055, s=0.00765)
p_"ADDRESS3" = custom_beta(u=0.084, s=0.02)

# Correct "ADDRESS1"/"ADDRESS2" probabilities to be a even split
def apply_prob_correction(p_"ADDRESS1", p_"ADDRESS2"):
    correction = p_"ADDRESS1" + p_"ADDRESS2"
    p_"ADDRESS1" = p_"ADDRESS1" / correction
    p_"ADDRESS2" = p_"ADDRESS2" / correction
    return (p_"ADDRESS1", p_"ADDRESS2")

p_"ADDRESS1", p_"ADDRESS2" = apply_prob_correction(p_"ADDRESS1", p_"ADDRESS2")

def apply_prob(df, prob):
    df = df.copy()
    draw_cols = df.columns[df.columns.str.contains("draw")]
    df[draw_cols] = df[draw_cols].apply(lambda x: x * prob, axis=1)
    return df

# Apply probabilities to base draws
df_"ADDRESS1" = apply_prob(df, p_"ADDRESS1")
df_"ADDRESS2" = apply_prob(df, p_"ADDRESS2")
df_"ADDRESS3" = apply_prob(df, p_"ADDRESS3")

def apply_duration(df, duration):
    prev = df.copy()
    draw_cols = df.columns[df.columns.str.contains("draw")]
    prev[draw_cols] = prev[draw_cols] * duration
    prev["measure_id"] = 5
    df = df.append(prev)
    return df

# Adjust incidence to prevalence w/ durations
# Source of durations: Whitehead et al, doi: 10.1038/nrmicro1690
df_"ADDRESS1" = apply_duration(df_"ADDRESS1", 6/365)
df_"ADDRESS2" = apply_duration(df_"ADDRESS2", 14/365)
df_"ADDRESS3" = apply_duration(df_"ADDRESS3", 0.5)
df_"ADDRESS1"["model_id"] = "ADDRESS1"
df_"ADDRESS2"["model_id"] = "ADDRESS2"
df_"ADDRESS3"["model_id"] = "ADDRESS3"


# Saves ("ADDRESS3" is prevalence only)
df_"ADDRESS1".to_csv(f"FILEPATH/ADDRESS1/{location_id}.csv", index=False)
df_"ADDRESS2".to_csv(f"FILEPATH/ADDRESS2/{location_id}.csv", index=False)
df_"ADDRESS3"[df_"ADDRESS3".measure_id==5].to_csv(f"FILEPATH/ADDRESS3/{location_id}.csv", index=False)

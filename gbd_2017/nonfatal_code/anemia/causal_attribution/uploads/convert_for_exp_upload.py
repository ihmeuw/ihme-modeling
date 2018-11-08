import pandas as pd
import argparse

indir = "FILEPATH"
outdir = "FILEPATH"


def exp_convert(location):
    df = pd.read_csv("{i}/exp_{l}.csv".format(i=indir, l=location))
    df.drop(df.columns[[0, 1]], axis=1)
    df = df[df["parameter"] == 'mean']
    renames = {'exp_%s' % d: 'draw_%s' % d for d in range(1000)}
    df.rename(columns=renames, inplace=True)
    df["measure_id"] = 19
    df.to_csv("{o}/{l}.csv".format(o=outdir, l=location))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location id to use", type=int)
    args = parser.parse_args()
    exp_convert(args.location)

import pandas as pd


def main():
    """
    Read, format, and save ICPC map.
    """

    df = pd.read_excel(FILEPATH, sheet_name="sunmmery1")

    print(df[df.bid1 == "FID3"])
    df.loc[df.bid1 == "FID3", 'bid1'] = 3059

    df = df.set_index('diagnosis').stack().reset_index()
    df = df[df[0] != 0]
    df = df[df[0] != "_none"]

    df.rename(columns={'level_1': 'bundle_level',
                       0: 'bundle_id'}, inplace=True)
    print(df.bundle_id.unique().size)
    # drop rows that are duplicated
    df = df.drop_duplicates()

    # Done
    filepath = FILEPATH
    df.to_csv(filepath, index=False)
    print(f"Saved ICPC map to {filepath}")
    print("Done.")


if __name__ == '__main__':
    main()

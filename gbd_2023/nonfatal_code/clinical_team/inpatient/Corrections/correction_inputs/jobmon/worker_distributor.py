"""Source-specific worker scripts define any ad-hoc processing while this unifies things
and allows us to use only one single-layer DAG to process each task."""

import argparse

from inpatient.CorrectionsFactors.correction_inputs.sources import (
    hcup_worker,
    nzl_worker,
    phl_worker,
)


def main_distribution(source: str, run_id: int, year: int, filepath: str) -> None:
    if source == "PHL_HICC":
        phl_worker.main(run_id=run_id, year=year)
    elif source == "NZL_NMDS":
        nzl_worker.main(run_id=run_id, year=year)
    elif source == "USA_HCUP_SID":
        hcup_worker.main(run_id=run_id, filepath=filepath)
    else:
        raise RuntimeError(f"Source {source} is not supported at this time.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Source to prep CF inputs.")
    parser.add_argument("--run_id", type=int, help="Clinical pipeline run id.")
    parser.add_argument("--filepath", type=str, help="File path of given cf parquet.")
    parser.add_argument("--year", type=int, help="The year of this subset of data.")

    args = parser.parse_args()

    main_distribution(
        source=args.source, run_id=args.run_id, year=args.year, filepath=args.filepath
    )

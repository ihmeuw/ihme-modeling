"""This is a CLI for the R wrapper for model launch.

Soon we'll replace this with Reticulate.
"""

from argparse import ArgumentParser, Namespace

import stgpr


def parse_args() -> Namespace:
    """Gets sendoff arguments from CLI."""
    parser = ArgumentParser()
    parser.add_argument("run_id", type=int)
    parser.add_argument("project", type=str)
    parser.add_argument("--log_path", type=str)
    parser.add_argument("--nparallel", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    """Launches model."""
    args = vars(parse_args())
    stgpr.stgpr_sendoff(**args)


if __name__ == "__main__":
    main()

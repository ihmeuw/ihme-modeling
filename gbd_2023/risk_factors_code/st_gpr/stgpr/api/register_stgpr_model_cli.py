"""This is a CLI for the R wrapper for registration.

Soon we'll replace this with Reticulate.
"""

from argparse import ArgumentParser, Namespace

import stgpr


def parse_args() -> Namespace:
    """Gets registration arguments from CLI."""
    parser = ArgumentParser()
    parser.add_argument("path_to_config", type=str)
    parser.add_argument("--model_index_id", type=int)
    return parser.parse_args()


def main() -> None:
    """Runs registration."""
    args = vars(parse_args())
    stgpr.register_stgpr_model(**args)


if __name__ == "__main__":
    main()

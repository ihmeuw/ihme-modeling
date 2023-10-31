import argparse
import dataclasses

import db_stgpr
from db_stgpr.api.enums import ModelStatus
from db_tools import ezfuncs

from stgpr_helpers.api import internal as stgpr_helpers_internal
from stgpr_helpers.api.constants import conn_defs


@dataclasses.dataclass
class PrepArgs:
    run_id: int


def parse_args() -> PrepArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    args = vars(parser.parse_args())
    return PrepArgs(**args)


def main() -> None:
    args = parse_args()
    with ezfuncs.session_scope(conn_defs.STGPR) as scoped_session:
        db_stgpr.update_model_status(args.run_id, ModelStatus.prep, scoped_session)

    with ezfuncs.session_scope(conn_defs.STGPR) as scoped_session:
        stgpr_helpers_internal.prep_data(args.run_id, scoped_session)

        db_stgpr.update_model_status(args.run_id, ModelStatus.stage1, scoped_session)


if __name__ == "__main__":
    main()

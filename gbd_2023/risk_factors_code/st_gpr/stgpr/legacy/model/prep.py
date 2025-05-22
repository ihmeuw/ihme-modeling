import argparse
import dataclasses

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema


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
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        db_stgpr.update_model_status(
            args.run_id, stgpr_schema.ModelStatus.prep, scoped_session
        )

    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        stgpr_helpers.configure_logging()
        stgpr_helpers.prep_data(args.run_id, scoped_session)

        db_stgpr.update_model_status(
            args.run_id, stgpr_schema.ModelStatus.stage1, scoped_session
        )


if __name__ == "__main__":
    main()

from argparse import ArgumentParser, Namespace
import datetime

from gbd.gbd_round import gbd_round_from_gbd_round_id
from save_results import save_results_epi


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--modelable_entity_id', type=int)
    parser.add_argument('--year_id', nargs='*', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--save_dir', type=str)
    return parser.parse_args()


def save_custom(modelable_entity_id, year_id, gbd_round_id, decomp_step,
                save_dir) -> int:
    description = (
        f"Anemia CA for GBD {gbd_round_from_gbd_round_id(gbd_round_id)} "
        f"{decomp_step}, {datetime.date.today().strftime('%m/%d/%y')}")
    save_results_epi(
        input_dir=save_dir,
        input_file_pattern="FILEPATH",
        modelable_entity_id=modelable_entity_id,
        description=description,
        year_id=year_id,
        measure_id=5,
        mark_best=True,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)


if __name__ == "__main__":
    args = parse_args()
    save_custom(
        modelable_entity_id=args.modelable_entity_id,
        year_id=args.year_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        save_dir=args.save_dir)

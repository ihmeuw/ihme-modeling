from save_results import save_results_risk
from argparse import ArgumentParser, Namespace
import datetime

today_string = datetime.date.today().strftime("%d/%m/%y")
years = [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022]


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--modelable_entity_id", help="modelable entity id to use", type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    return parser.parse_args()

def save_tmrel_draws(modelable_entity_id, gbd_round_id, decomp_step) -> int:
    description = (
    f"Iron TMREL Draws for GBD 2020, Round {gbd_round_id}"
    f" {decomp_step}, {datetime.date.today().strftime('%d/%m/%y')}")
    save_results_risk(
        input_dir='FILEPATH',
        input_file_pattern="{location_id}.csv",
        modelable_entity_id=modelable_entity_id,
        description=description,
        risk_type='tmrel',
        year_id=years,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        mark_best=True
        )

if __name__ == "__main__":
    args = parse_args()
    save_tmrel_draws(
        modelable_entity_id=args.modelable_entity_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)

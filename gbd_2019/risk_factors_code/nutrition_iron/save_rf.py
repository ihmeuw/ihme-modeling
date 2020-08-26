import sys;print(sys.executable)
import datetime
from save_results import save_results_epi
from argparse import ArgumentParser, Namespace


pairings = {19821: 'other_iron', 19822: 'low_hgb', 19823: 'low_hgb_iron_responsive', 8882: 'nutrition_iron'}

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--modelable_entity_id", help="modelable entity id to use", type=int)
    parser.add_argument("--year_id", nargs='*', type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    return parser.parse_args()



def save_anem_exposures(modelable_entity_id, year_id, gbd_round_id, decomp_step) -> int:
    description = (
        f"Calculated {pairings[modelable_entity_id]} exposure from anemia causal attribution results"
        f" {decomp_step}, {datetime.date.today().strftime('%m/%d/%y')}"
	)
    save_results_epi(
        input_dir='FILEPATH',
        input_file_pattern='FILEPATH',
        measure_id= 19,
	metric_id= 3,
        modelable_entity_id=modelable_entity_id,
        description='Calculated {} exposure from anemia causal attribution results'.format(pairings[modelable_entity_id]),
        year_id=year_id,
	decomp_step=decomp_step,
        gbd_round_id=gbd_round_id,
        mark_best=True)


if __name__ == "__main__":
    args = parse_args()
    save_anem_exposures(
        modelable_entity_id=args.modelable_entity_id,
        year_id=args.year_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)

import sys;print(sys.executable)
import datetime
from save_results import save_results_epi
from argparse import ArgumentParser, Namespace


pairings = {19821: 'other_iron', 19822: 'low_hgb', 19823: 'low_hgb_iron_responsive', 8882: 'nutrition_iron'}
years = [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022]

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--modelable_entity_id", help="modelable entity id to use", type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    return parser.parse_args()



def save_anem_exposures(modelable_entity_id, gbd_round_id, decomp_step) -> int:
    description = (
        f"Calculated {pairings[modelable_entity_id]} exposure from anemia causal attribution results"
        f" {decomp_step}, {datetime.date.today().strftime('%m/%d/%y')}"
	)
    save_results_epi(
        input_dir='FILEPATH',
        input_file_pattern='{location_id}.csv',
        measure_id= 19,
	    metric_id= 3,
        modelable_entity_id=modelable_entity_id,
        description=description,
        year_id=years,
	    decomp_step=decomp_step,
        bundle_id=94,
        crosswalk_version_id=18992,
        gbd_round_id=gbd_round_id,
        mark_best=True)


if __name__ == "__main__":
    args = parse_args()
    save_anem_exposures(
        modelable_entity_id=args.modelable_entity_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)

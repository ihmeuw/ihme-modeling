from argparse import ArgumentParser, Namespace
import datetime

from save_results import save_results_epi


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--modelable_entity_id', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--out_dir', type=str)
    return parser.parse_args()


def save_malaria(modelable_entity_id, gbd_round_id, decomp_step,
                 out_dir) -> int:
    today_string = datetime.date.today().strftime("%m/%d/%y")
    mv = save_results_epi(
        input_dir=f'FILEPATH',
        input_file_pattern="FILEPATH",
        modelable_entity_id=modelable_entity_id,
        description=f'malaria anemia pre process {today_string}',
        measure_id=5,
        metric_id=3,
        mark_best=True,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)
    return mv


if __name__ == "__main__":
    args = parse_args()
    save_malaria(
        modelable_entity_id=args.modelable_entity_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        out_dir=args.out_dir)

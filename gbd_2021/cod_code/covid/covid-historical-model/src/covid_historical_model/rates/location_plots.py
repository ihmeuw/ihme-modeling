import sys
from pathlib import Path
import dill as pickle

import pandas as pd

from covid_shared.cli_tools.logging import configure_logging_to_terminal

from covid_historical_model.rates.ifr.plotter import plotter as ifr_plotter
from covid_historical_model.rates.serology import plotter as sero_plotter


def main(location_id: int,
         inputs_path: str, plots_dir: str):
    with Path(inputs_path).open('rb') as file:
        inputs = pickle.load(file)
    hierarchy = inputs['hierarchy']
    location_name = hierarchy.loc[hierarchy['location_id'] == location_id, 'location_name'].item()
        
    ifr_plotter(location_id, location_name,
                Path(plots_dir) / f'{location_id}_ifr.pdf',
                **inputs)

    if location_id in inputs['ifr_results'].seroprevalence['location_id'].to_list():
        sero_plotter(location_id, location_name,
                     Path(plots_dir) / f'{location_id}_sero.pdf',
                     **inputs)


if __name__ == '__main__':
    configure_logging_to_terminal(verbose=2)
    
    main(int(sys.argv[1]), sys.argv[2] , sys.argv[3])

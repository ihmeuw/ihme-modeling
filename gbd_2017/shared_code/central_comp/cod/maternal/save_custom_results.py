"""
Author: author
Date: date
Description: Using the new save results script for GBD 2017. Saves for either epi, cod, or SDG
			 depending on the arguments.
"""
import sys
import logging

from save_results import save_results_cod, save_results_epi, save_results_sdg

########################################################################
# Grab the arguments needed for save results and set up logging
########################################################################

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.save_results")
print(sys.argv)

model_id, directory, file_pattern, start_year, end_year, model_type, description = sys.argv[1:8]

model_id = int(model_id)
start_year = int(start_year)
end_year = int(end_year)

logger.info("Saving results for maternal {type}".format(type=model_type))

########################################################################
# Save results
########################################################################

if model_type == "cod":
    print("Saving results for COD")
    save_results_cod(input_dir=directory, input_file_pattern=file_pattern, cause_id=model_id,
                     description=description,
                     sex_id=2, year_id=range(start_year, end_year + 1), mark_best=True)
elif model_type == "epi":
    print("Saving results for EPI")
    save_results_epi(input_dir=directory, input_file_pattern=file_pattern, modelable_entity_id=model_id,
                     description=description,
                     sex_id=2, year_id=range(start_year, end_year + 1),
                     measure_id=18, mark_best=True)
elif model_type == "sdg":
	print("Saving results for SDG")
	save_results_sdg(input_dir=directory, input_file_pattern=file_pattern, indicator_component_id=47,
					 gbd_id=366, gbd_id_type='cause_id',
					 input_version_id=model_id, n_draws=1000)
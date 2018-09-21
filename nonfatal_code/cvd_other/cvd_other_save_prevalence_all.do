clear all
set more off

adopath + "FILEPATH"

get_best_model_versions, entity(modelable_entity) ids(9575) clear
levelsof model_version_id, local(models)

quietly do FILEPATH/save_results.do

save_results, modelable_entity_id(2908) description("other CVD after MEPS ratio calc; `models'") in_dir("FILEPATH") metrics(5) mark_best(yes)

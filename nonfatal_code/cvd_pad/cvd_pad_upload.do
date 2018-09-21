adopath + "FILEPATH"

get_best_model_versions, entity(modelable_entity) ids(1861 2532) clear
levelsof model_version_id, local(models)

quietly do "FILEPATH/save_results.do"

save_results, modelable_entity_id(3118) description("upload 6/21; splits from `models'") in_dir("FILEPATH") metrics(5 6) mark_best(yes)
save_results, modelable_entity_id(3234) description("upload 6/21; splits from `models'") in_dir("FILEPATH") metrics(5) mark_best(yes)

//Pulls best model numbers for relevant mes; uploads using save results
adopath + FILEPATH

get_best_model_versions, entity(modelable_entity) ids(1814 9567 15755) clear
levelsof model_version_id, local(models)

quietly do FILEPATH/save_results.do

save_results, modelable_entity_id(3233) description("asymptomatic post mi after angina (ages corrected), HF adjustment; best models `models'") in_dir("FILEPATH") metrics(5) mark_best(yes)

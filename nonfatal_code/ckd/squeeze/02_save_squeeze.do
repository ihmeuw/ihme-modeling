// source save results function 
run FILEPATH
local output_version 6

save_results, modelable_entity_id(10732) mark_best("yes") in_dir(FILEPATH) description("MVID 175661 stage 3") file_pattern("{location_id}.csv")
save_results, modelable_entity_id(10733) mark_best("yes") in_dir(FILEPATH) description("MVID 172949 stage 4") file_pattern("{location_id}.csv")
save_results, modelable_entity_id(10734) mark_best("yes") in_dir(FILEPATH) description("MVID 173468 stage 5") file_pattern("{location_id}.csv")

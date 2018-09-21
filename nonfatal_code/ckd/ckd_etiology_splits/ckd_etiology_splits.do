// source save_results and split_epi_model central functions 
run FILEPATH
run FILEPATH

//stage3 
split_epi_model, source_meid(10732) target_meids( 3029 3030 3031 3032 ) prop_meids( 9971 9973 9972 9974) clear

save_results, modelable_entity_id(3029) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(3030) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(3031) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(3032) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5") 

//stage4
split_epi_model, source_meid(10733) target_meids( 11595 11597 11599 11601 ) prop_meids( 9971 9973 9972 9974 ) split_meas_ids(5) clear 

save_results, modelable_entity_id(11595) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11597) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11599) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11601) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")


//stage5
split_epi_model, source_meid(10734) target_meids( 11596 11598 11600 11602 ) prop_meids(9971 9973 9972 9974) split_meas_ids(5) clear 

save_results, modelable_entity_id(11596) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11598) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11600) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(11602) mark_best("yes") in_dir(FILEPATH) description("splits 060517") h5_tablename("draws") file_pattern("{location_id}.h5")

//dialysis
split_epi_model, source_meid(2021) target_meids ( 2026 2034 2042 2050 ) prop_meids (9971 9973 9972 9974) split_meas_ids(5) clear

save_results, modelable_entity_id(2026) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2034) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2042) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2050) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")

//transplant
split_epi_model, source_meid(2020) target_meids ( 2025 2033 2041 2049 ) prop_meids (9971 9973 9972 9974) split_meas_ids(5) clear 

save_results, modelable_entity_id(2025) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2033) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2041) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")
save_results, modelable_entity_id(2049) mark_best("yes") in_dir(FILEPATH) description("splits 061517") h5_tablename("draws") file_pattern("{location_id}.h5")


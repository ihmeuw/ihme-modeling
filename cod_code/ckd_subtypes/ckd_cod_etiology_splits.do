
run "FILEPATH/save_results.do"
run "FILEPATH/split_cod_model.ado"

split_cod_model, source_cause_id(589) target_cause_ids(590 591 592 593) target_meids( 9971 9973 9972 9974) output_dir("FILEPATH") clear

save_results, cause_id(590) in_rate("no") mark_best("yes") description("cod splits using ESRD model, 6/5/17") in_dir("FILEPATH")
save_results, cause_id(591) in_rate("no") mark_best("yes") description("cod splits using ESRD model, 6/5/17") in_dir("FILEPATH")
save_results, cause_id(592) in_rate("no") mark_best("yes") description("cod splits using ESRD model, 6/5/17") in_dir("FILEPATH")
save_results, cause_id(593) in_rate("no") mark_best("yes") description("cod splits using ESRD model, 6/5/17") in_dir("FILEPATH")



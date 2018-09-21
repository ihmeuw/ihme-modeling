//////////////////////////////////////////
// Title: save hiv-ipv paf results
// Notes: change description & directory
//////////////////////////////////////////

do "FILEPATH/save_results.do" // load save_results shared function

save_results, modelable_entity_id(8824) description("DESCRIPTION") in_dir("FILEPATH") risk_type("paf") mark_best("yes")  morbidity("yes") mortality("yes") sexes(2)

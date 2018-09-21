
*pull draws of age trend from dismod model


adopath ++ "FILEPATH"
create_connection_string, database(gbd) server(modeling-gbd-db)
    local gbd_str = r(conn_string)



get_draws, gbd_id_field(modelable_entity_id) gbd_id(11649) location_ids(1) year_ids(2010) status(latest) source(epi) 

save "FILEPATH/draws.dta", replace

*apply draws (which are in prevalence space) to our incidence data

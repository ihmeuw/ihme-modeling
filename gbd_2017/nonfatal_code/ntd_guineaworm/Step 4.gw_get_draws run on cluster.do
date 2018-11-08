
*pull draws


adopath ++ "FILEPATH"
create_connection_string, database(ADDRESS) server(ADDRESS)
    local gbd_str = r(conn_string)



get_draws, gbd_id_field(modelable_entity_id) gbd_id(11649) location_ids(1) year_ids(2010) status(latest) source(ADDRESS) 

save "FILEPATH/draws.dta", replace

*apply draws (which are in prevalence space) to our incidence data

library(data.table)
library(RMySQL)
library(ini)
library(knitr)
library(magrittr)

# query helper
query <- function(query, conn_def) {
    odbc <- read.ini("FILEPATH/.odbc.ini")
    conn <- dbConnect(RMySQL::MySQL(),
                    host = odbc[[conn_def]]$server,
                    username = odbc[[conn_def]]$user,
                    password = odbc[[conn_def]]$password)
    dt <- dbGetQuery(conn, query) %>% data.table
    dbDisconnect(conn)
    return(dt)
}

get_decomp_step_id <- function(step, round_id) {
    dt <- query("SELECT decomp_step_id, decomp_step_name, gbd_round_id
                FROM shared.decomp_step
                WHERE decomp_step_id <> 8", "shared")
    dt[!grepl("Step",decomp_step_name), decomp_step_name := "iterative"]
    dt[, decomp_step_name := tolower(gsub(" ", "", decomp_step_name))]
    return(dt[decomp_step_name==step & gbd_round_id==round_id]$decomp_step_id)
}

# pull risk_variable metadata
get_rei_meta <- function(rid) {
    metadata <- fread("risk_metadata.csv")[rei_id == rid, ]
    return(list(rei = unique(metadata$rei), calc_type = unique(metadata$rei_calculation_type),
                female = unique(metadata$female), male = unique(metadata$male),
                yll = unique(metadata$yll), yld = unique(metadata$yld),
                exp_dist = unique(metadata$exposure_type), inv_exp = unique(metadata$inv_exp),
                rr_scalar = unique(metadata$rr_scalar), tmrel_dist = unique(metadata$tmred_dist),
                tmrel_lower = unique(metadata$tmrel_lower), tmrel_upper = unique(metadata$tmrel_upper),
                yll_age_group_id_start = unique(metadata$yll_age_group_id_start),
                yll_age_group_id_end = unique(metadata$yll_age_group_id_end),
                yld_age_group_id_start = unique(metadata$yld_age_group_id_start),
                yld_age_group_id_end = unique(metadata$yld_age_group_id_end),
                rei_id = rid))
}

# modelable_entity_id and best model_version_id for each draw type for a given risk
get_rei_mes <- function(rei_id, gbd_round_id, decomp_step) {
    decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
    dt <- query(paste0(
        "SELECT
             rei_id, modelable_entity_id, modelable_entity_name,
             dtype.modelable_entity_metadata_value as draw_type,
             param.modelable_entity_metadata_value as exp_categ,
             model_version_id
         FROM epi.modelable_entity me
         JOIN epi.modelable_entity_rei mer using(modelable_entity_id)
         LEFT JOIN
             (SELECT *
             FROM epi.model_version
             WHERE model_version_status_id = 1
             AND gbd_round_id = ", gbd_round_id, " AND
             decomp_step_id = ", decomp_step_id, ") mv USING (modelable_entity_id)
         JOIN
             (SELECT
             modelable_entity_metadata_value, modelable_entity_id
             FROM epi.modelable_entity_metadata mem
             JOIN epi.modelable_entity_metadata_type memt
             using (modelable_entity_metadata_type_id)
             WHERE modelable_entity_metadata_type = 'draw_type'
             AND mem.last_updated_action != 'DELETE') dtype
             using (modelable_entity_id)
         LEFT JOIN
             (SELECT
             modelable_entity_metadata_value, modelable_entity_id
             FROM epi.modelable_entity_metadata mem
             JOIN epi.modelable_entity_metadata_type memt
             using (modelable_entity_metadata_type_id)
             WHERE modelable_entity_metadata_type = 'categorical_parameter'
             AND mem.last_updated_action != 'DELETE') param
             using (modelable_entity_id)
         JOIN
             (SELECT
             modelable_entity_metadata_value, modelable_entity_id
             FROM
             epi.modelable_entity_metadata mem
             JOIN
             epi.modelable_entity_metadata_type
             using (modelable_entity_metadata_type_id)
             WHERE
             modelable_entity_metadata_type = 'gbd_2019'
             and mem.last_updated_action != 'DELETE') round
             using (modelable_entity_id)
         WHERE
            me.last_updated_action != 'DELETE' and
            me.end_date is NULL and rei_id = ", rei_id), "epi")
    if (rei_id == 140) {
        drugs <- query(paste0(
        "SELECT modelable_entity_id, modelable_entity_name, model_version_id
         FROM epi.modelable_entity me
         LEFT JOIN
             (SELECT *
             FROM epi.model_version
             WHERE model_version_status_id = 1
             AND gbd_round_id = ", gbd_round_id,
             " AND decomp_step_id = ", decomp_step_id, ") mv USING (modelable_entity_id)
         WHERE modelable_entity_id in (24644, 24645, 24646)"), "epi")
      drugs[, rei_id := 140][, draw_type := "exposure"]
      dt <- rbind(dt, drugs, fill = T)
    }
    if (rei_id == 243) dt <- rbind(dt, get_rei_mes(107, gbd_round_id, decomp_step)[draw_type == "rr"][, rei_id := 243])
    if (rei_id == 380) dt <- rbind(dt[draw_type != "rr"], get_rei_mes(339, gbd_round_id, decomp_step)[draw_type == "rr"][, rei_id := 380])
    if (rei_id == 95) {
      iron <- query(paste0(
        "SELECT modelable_entity_id, modelable_entity_name, model_version_id
         FROM epi.modelable_entity me
         LEFT JOIN
             (SELECT *
             FROM epi.model_version
             WHERE model_version_status_id = 1
             AND gbd_round_id = ", gbd_round_id,
             " AND decomp_step_id = ", decomp_step_id, ") mv USING (modelable_entity_id)
         WHERE modelable_entity_id = 10488"), "epi")
      iron[, rei_id := 95][, draw_type := "exposure_sd"]
      dt <- rbind(dt, iron, fill = T)
    }
    return(dt[order(draw_type, exp_categ)])
}

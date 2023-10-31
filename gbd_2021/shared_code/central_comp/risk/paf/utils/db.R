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
                FROM shared.decomp_step", "shared")
    dt[decomp_step_name == "Iterative" | gbd_round_id < 6,
       decomp_step_name := "iterative"]
    dt[, decomp_step_name := tolower(gsub(" ", "", decomp_step_name))]
    dt <- dt[decomp_step_name==step & gbd_round_id==round_id]
    if(nrow(dt) == 0) stop("decomp_step_id not found for decomp_step ", step, " and gbd_round_id ", round_id)
    return(dt$decomp_step_id)
}

get_me_metadata_type_id <- function(gbd_round_id) {
    dt <- query('SELECT modelable_entity_metadata_type_id, gbd_round_id as gbd
                FROM epi.modelable_entity_metadata_type memt
                JOIN shared.gbd_round ON replace(memt.modelable_entity_metadata_type,
                    "gbd_","") = gbd_round.gbd_round
                COLLATE utf8_unicode_ci', "epi")
    return(dt[gbd == gbd_round_id, ]$modelable_entity_metadata_type_id)
}

# pull risk_variable metadata
get_rei_meta <- function(rei_id, gbd_round_id) {
    metadata <- query(paste0(
        "SELECT
            rmh.rei_id, rmh.rei, rmt.rei_metadata_type, rmh.rei_metadata_value
        FROM shared.rei_metadata_type rmt
        LEFT JOIN (
            SELECT rei_id, rei, rei_metadata_type_id, rei_metadata_value
            FROM shared.rei_metadata_version_active rmv
            JOIN shared.rei_metadata_history rmh USING(rei_metadata_version_id)
            JOIN shared.rei r USING(rei_id)
            WHERE rmv.gbd_round_id = ", gbd_round_id, " AND r.rei_id = ", rei_id, ") rmh USING(rei_metadata_type_id)
        WHERE
            rmt.rei_metadata_type_id BETWEEN 8 AND 23 OR
            rmt.rei_metadata_type_id = 27"), "shared")
    if(nrow(metadata[!is.na(rei_id), ]) == 0)
        stop("REI metadata in shared not found for rei_id ", rei_id, " and gbd_round_id ", gbd_round_id)
    metadata_list <- setNames(as.list(metadata$rei_metadata_value), metadata$rei_metadata_type)
    # convert numeric metdata types from strings to numbers
    numeric_metadata <- c("rei_calculation_type", "inv_exp", "rr_scalar",
                          "tmrel_lower", "tmrel_upper", "female", "male",
                          "yld", "yld_age_group_id_start", "yld_age_group_id_end",
                          "yll", "yll_age_group_id_start", "yll_age_group_id_end",
                          "age_specific_exp")
    for(i in 1:length(metadata_list)) {
        if (names(metadata_list[i]) %in% numeric_metadata)
            metadata_list[i] <- as.numeric(metadata_list[i])
    }
    metadata_list[["rei_id"]] <- rei_id
    metadata_list[["rei"]] <- unique(metadata[!is.na(rei)]$rei)
    if (metadata_list$rei %like% "^diet_") {
        diet_tmrel <- fread("FILEPATH/2019_tmrels.csv")[rei_id == metadata_list$rei_id, ]
        metadata_list$tmrel_lower <- diet_tmrel$tmrel_lower
        metadata_list$tmrel_upper <- diet_tmrel$tmrel_upper
    }
    return(metadata_list)
}

# modelable_entity_id and best model_version_id for each draw type for a given risk
get_rei_mes <- function(rei_id, gbd_round_id, decomp_step) {
    decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
    me_metadata_type_id <- get_me_metadata_type_id(gbd_round_id)
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
             (SELECT modelable_entity_metadata_value, modelable_entity_id
             FROM epi.modelable_entity_metadata
             WHERE modelable_entity_metadata_type_id = 17
             AND modelable_entity_metadata_value IN
             ('paf', 'exposure', 'exposure_sd', 'rr', 'tmrel', 'paf_unmediated')
             AND last_updated_action != 'DELETE') dtype using (modelable_entity_id)
         LEFT JOIN
             (SELECT modelable_entity_metadata_value, modelable_entity_id
             FROM epi.modelable_entity_metadata
             WHERE modelable_entity_metadata_type_id = 20
             AND last_updated_action != 'DELETE') param using (modelable_entity_id)
         JOIN
             (SELECT modelable_entity_metadata_value, modelable_entity_id
             FROM epi.modelable_entity_metadata
             WHERE modelable_entity_metadata_type_id  = ", me_metadata_type_id, "
             and last_updated_action != 'DELETE') round using (modelable_entity_id)
         WHERE
            me.last_updated_action != 'DELETE' and
            me.end_date is NULL and rei_id = ", rei_id), "epi")
    # occ asbestos has two PAF modelable entities, drop the one that is directly modeled
    if (rei_id == 150) dt <- dt[modelable_entity_id != 16425, ]
    # particulate matter pollution uses lbw/sg RRs (but not through two stage mediation)
    if (rei_id == 380)
        dt <- rbind(dt[draw_type != "rr"],
                    get_rei_mes(339, gbd_round_id, decomp_step)[draw_type == "rr"][, rei_id := 380]
        )
    # add iron exposure SD
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

# Get df of causes and their draw types for a given relative risk model version
get_rr_metadata <- function(model_version_id) {
    rr_metadata <- query(paste0(
        "SELECT rei_id, cause_id, relative_risk_type_id, model_version_id
         FROM epi.relative_risk_metadata
         WHERE model_version_id = ", model_version_id), "epi")
    return(rr_metadata)
}

# Get the bundle shape for a crosswalk version so we can infer columns
get_cw_bundle_shape <- function(crosswalk_version_id) {
    shape_id <- query(paste0(
        "SELECT bs.shape_id
         FROM crosswalk_version.crosswalk_version cwv
         JOIN bundle_version.bundle_version bv USING (bundle_version_id)
         JOIN bundle.bundle_shape bs USING (bundle_id)
         WHERE crosswalk_version_id = ", crosswalk_version_id), "epi")
    return(shape_id)
}

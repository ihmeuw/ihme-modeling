source("FILEPATH/interpolate.R")

# hearing sequela to cause for occ noise
convert_hearing <- function(dt, location_id, year_id, sex_id, gbd_round_id, n_draws) {

    #---- SET CAUSE MAPS -------------------------------------------------------

    # the cause_id in the PAF is actually the sequela_id
    setnames(dt, "cause_id", "sequela_id")
    sequela_ids <- unique(dt$sequela_id)
    ages <- unique(dt$age_group_id)

    # map between hearing sequela, and cause
    cid <- 674
    hearing_map <- query(sprintf(
        "SELECT sequela_id, cause_id
        FROM epi.sequela_hierarchy_history
        JOIN (SELECT sequela_set_version_id
              FROM epi.sequela_set_version_active
              WHERE gbd_round_id = %s
                AND sequela_set_id = 2) ssvd USING(sequela_set_version_id)
        WHERE cause_id = %s", gbd_round_id, cid), "epi")

    #-- YLD ----------------------------------------------------------------------

    # pull ylds for all the hearing sequela
    seq_yld <- rbindlist(lapply(sequela_ids, function(x)
        interpolate(gbd_id_type = "sequela_id", gbd_id = x, location_id = location_id,
                    sex_id = sex_id, measure_id = 3, gbd_round_id = gbd_round_id,
                    source = "como", version_id =287)), use.names = T)
    seq_yld <- melt(seq_yld, id.vars = c("sequela_id", "location_id", "year_id",
                                         "age_group_id", "sex_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "seq")
    seq_yld[, draw := as.numeric(gsub("draw_", "", draw))]
    seq_yld <- merge(seq_yld, hearing_map, by="sequela_id")
    # find total YLD across sequela
    seq_yld[, seq_total := sum(seq), by = c("cause_id", "location_id", "year_id",
                                            "age_group_id", "sex_id", "draw")]

    # pull ylds for hearing cause
    cause_yld <- interpolate(gbd_id_type = "cause_id", gbd_id = cid,
                             location_id = location_id, sex_id = sex_id, measure_id = 3,
                             gbd_round_id = gbd_round_id, source = "como", version_id =287)
    cause_yld <- melt(cause_yld, id.vars = c("cause_id", "location_id", "year_id",
                                             "age_group_id", "sex_id"),
                      measure.vars = paste0("draw_", 0:(n_draws - 1)),
                      variable.name = "draw", value.name = "cause")
    cause_yld[, draw := as.numeric(gsub("draw_", "", draw))]

    # combine YLDs and PAFs
    yld_dt <- merge(dt, seq_yld, by = c("sequela_id", "location_id", "year_id",
                                        "age_group_id", "sex_id", "draw"))
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))

    # scale PAF by sequela/cause proportion
    yld_dt[, seq := cause * seq/seq_total][seq_total == 0, seq := 0]
    yld_dt[, paf := paf * seq]
    # collapse over sequela
    yld_dt <- yld_dt[, .(paf = sum(paf)), by = c("cause_id", "location_id", "year_id",
                                                 "age_group_id", "sex_id", "draw")]
    # divide by original # ylds by cause
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))
    yld_dt[, paf := paf/cause][cause == 0, paf := 0][, cause := NULL]

    yld_dt <- yld_dt[age_group_id %in% ages]
    yld_dt[, morbidity := 1][, mortality := 0]
    return(yld_dt)
}

# for BMI osteoarthritis YLD outcome
convert_osteo <- function(dt, location_id, year_id, sex_id, gbd_round_id, n_draws) {

    # subest paf dt to only where we have osteoarthritis YLD outcomes
    osteo_paf <- dt[cause_id %in% c(2141, 2145) & morbidity == 1, ]
    setnames(osteo_paf, "cause_id", "parent_id")
    ages <- unique(osteo_paf$age_group_id)

    #---- SET CAUSE MAPS -------------------------------------------------------

    # map between osteoarthritis ME (id in RRs), sequela, and cause
    cause_id <- 628
    osteo_map <- query(sprintf(
        "SELECT parent_id, sequela_name, sequela_id, cause_id
        FROM epi.sequela_hierarchy_history
        JOIN (SELECT sequela_set_version_id
        FROM epi.sequela_set_version_active
        WHERE gbd_round_id = %s
        AND sequela_set_id = 2) ssvd USING(sequela_set_version_id)
        WHERE cause_id = %s", gbd_round_id, cause_id),
        "epi"
    )
    osteo_map[sequela_name %like% "osteoarthritis of the hip", parent_id := 2141]
    osteo_map[sequela_name %like% "osteoarthritis of the knee", parent_id := 2145]
    osteo_map <- osteo_map[, .(sequela_id, cause_id, parent_id)]
    sequela_ids <- unique(osteo_map$sequela_id)

    #-- YLD ----------------------------------------------------------------------

    # pull ylds for all the osteoarthritis sequela
    seq_yld <- rbindlist(lapply(sequela_ids, function(x)
        interpolate(gbd_id_type = "sequela_id", gbd_id = x, location_id = location_id,
                    sex_id = sex_id, measure_id = 3, gbd_round_id = gbd_round_id,
                    source = "como", version_id =287)), use.names = T)
    seq_yld <- melt(seq_yld, id.vars = c("sequela_id", "location_id", "year_id",
                                         "age_group_id", "sex_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "seq")
    seq_yld[, draw := as.numeric(gsub("draw_", "", draw))]
    seq_yld <- merge(seq_yld, osteo_map, by="sequela_id")
    # find total YLD across sequela
    seq_yld[, seq_total := sum(seq), by = c("cause_id", "location_id", "year_id",
                                            "age_group_id", "sex_id", "draw")]

    # pull ylds for osteoarthritis cause
    cause_yld <- interpolate(gbd_id_type = "cause_id", gbd_id = cause_id,
                             location_id = location_id, sex_id = sex_id, measure_id = 3,
                             gbd_round_id = gbd_round_id, source = "como", version_id =287)
    cause_yld <- melt(cause_yld, id.vars = c("cause_id", "location_id", "year_id",
                                             "age_group_id", "sex_id"),
                      measure.vars = paste0("draw_", 0:(n_draws - 1)),
                      variable.name = "draw", value.name = "cause")
    cause_yld[, draw := as.numeric(gsub("draw_", "", draw))]

    # combine YLDs and PAFs
    yld_dt <- merge(osteo_paf, seq_yld, by = c("parent_id", "location_id", "year_id",
                                               "age_group_id", "sex_id", "draw"))
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))

    # scale PAF by sequela/cause proportion
    yld_dt[, seq := cause * seq/seq_total][seq_total == 0, seq := 0]
    yld_dt[, paf := paf * seq]
    # collapse over sequela
    yld_dt <- yld_dt[, .(paf = sum(paf)), by = c("cause_id", "location_id", "year_id",
                                                 "age_group_id", "sex_id", "draw")]
    # divide by original # ylds by cause
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))
    yld_dt[, paf := paf/cause][cause == 0, paf := 0][, cause := NULL]
    yld_dt <- yld_dt[age_group_id %in% ages]
    yld_dt[, morbidity := 1][, mortality := 0]

    #-- RETURN ALL PAFS --------------------------------------------------------

    return(rbind(dt[!(cause_id %in% c(2141, 2145))], yld_dt,
                 fill=T, use.names = T))

}


source("FILEPATH/interpolate.R")
source("FILEPATH/get_sequela_metadata.R")

# hearing sequela to cause for occ noise
convert_hearing <- function(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws) {

    #---- SET CAUSE MAPS -------------------------------------------------------

    # the cause_id in the PAF is actually the sequela_id
    setnames(dt, "cause_id", "sequela_id")
    sequela_ids <- unique(dt$sequela_id)
    ages <- unique(dt$age_group_id)

    # map between hearing sequela, and cause
    cid <- 674
    hearing_map <- get_sequela_metadata(sequela_set_id = 2, gbd_round_id = gbd_round_id)
    hearing_map <- hearing_map[cause_id == cid & most_detailed ==1, .(sequela_id, cause_id)]

    #-- YLD ----------------------------------------------------------------------

    # pull ylds for all the hearing sequela
    seq_yld <- rbindlist(lapply(sequela_ids, function(x)
        get_draws(gbd_id_type = "sequela_id", gbd_id = x, location_id = location_id,
                  year_id = year_id, sex_id = sex_id, measure_id = 3,
                  gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                  source = "como", n_draws = n_draws, downsample = TRUE)), use.names = T)
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
    cause_yld <- get_draws(gbd_id_type = "cause_id", gbd_id = cid, year_id = year_id,
                           location_id = location_id, sex_id = sex_id, measure_id = 3,
                           gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                           source = "como", n_draws = n_draws, downsample = TRUE)
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

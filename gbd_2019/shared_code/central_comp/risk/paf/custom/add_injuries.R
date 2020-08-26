# for BMD and smoking injuries
add_injuries <- function(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws) {

    # subest paf dt to only where we have hip/non-hip fractures to turn to injuries
    inj_paf <- dt[cause_id %in% c(878, 923), ]
    inj_paf[, fracture := ifelse(cause_id == 878, "hip", "non-hip")][, cause_id := NULL]
    ages <- unique(inj_paf$age_group_id)

    #---- SET CAUSE MAPS -------------------------------------------------------

    # acause and cause_ids for injury outcomes we use
    acauses <- c("inj_trans_road_pedest", "inj_trans_road_pedal", "inj_trans_road_2wheel",
                 "inj_trans_road_4wheel", "inj_trans_road_other", "inj_trans_other",
                 "inj_falls", "inj_mech_other", "inj_animal_nonven", "inj_homicide_other")
    cause_dt <- get_cause_metadata(cause_set_id = 2, gbd_round_id = gbd_round_id)
    cause_dt <- cause_dt[acause %in% acauses, .(cause_id, acause)]
    cause_ids <- unique(cause_dt$cause_id)

    #-- CONVERT ----------------------------------------------------------------

    # pull proportion hospital deaths
    hosp_deaths <- fread("FILEPATH/gbd2017_proportions_of_hospital_deaths.csv")
    setnames(hosp_deaths, c("acause", "inj"), c("fracture", "acause"))
    hosp_deaths <- hosp_deaths[acause %in% acauses, .(sex_id, age_group_id, fracture,
                                                      acause, fraction)]
    hosp_deaths <- merge(hosp_deaths[, c("sex_id", "age_group_id", "acause", "fracture",
                                         "fraction"), with=F], cause_dt, by = "acause")

    # pull deaths for all the injuries causes
    inj_yll <- get_draws(gbd_id_type = rep("cause_id", length(cause_ids)),
                         gbd_id = cause_ids, location_id = location_id,
                         year_id = year_id, sex_id = sex_id, measure_id = 1,
                         gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                         source = "codcorrect", n_draws = n_draws, downsample = TRUE)
    inj_yll <- melt(inj_yll, id.vars = c("cause_id", "location_id", "year_id",
                                         "age_group_id", "sex_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "yll")
    inj_yll[, draw := as.numeric(gsub("draw_", "", draw))]

    # combine deaths and hosp deaths and PAFs
    inj_paf <- merge(inj_paf[mortality == 1, ], hosp_deaths,
                    by = c("fracture", "age_group_id", "sex_id"), allow.cartesian = T)
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                            "age_group_id", "sex_id", "draw"))
    inj_paf[, paf := paf * yll * fraction]
    # collapse over hip non hip
    inj_paf <- inj_paf[, .(paf = sum(paf)), by = c("cause_id", "location_id", "year_id",
                                                 "age_group_id", "sex_id", "draw")]
    # divide by original # deaths by cause
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                            "age_group_id", "sex_id", "draw"))
    inj_paf[, paf := paf/yll][yll == 0, paf := 0][, yll := NULL]
    inj_paf <- inj_paf[age_group_id %in% ages]
    inj_paf[, morbidity := 1][, mortality := 1]

    #-- RETURN ALL PAFS --------------------------------------------------------

    return(rbindlist(list(dt[!(cause_id %in% c(878, 923)), ],
                       inj_paf), fill=T, use.names = T))

}

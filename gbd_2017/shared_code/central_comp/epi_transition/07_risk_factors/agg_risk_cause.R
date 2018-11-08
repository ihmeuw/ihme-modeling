agg_risk <- function(data, id_vars, gbd_round_id, med = T) {

    # make rei map with parent and children risks
    reis <- get_rei_metadata(rei_set_id = 1, gbd_round_id = gbd_round_id)
    parent_cols <- paste0("parent_",min(reis$level):max(reis$level))
    reis <- reis[, .(rei_id, path_to_top_parent, most_detailed)]
    reis[, (parent_cols) := tstrsplit(path_to_top_parent, ",")][, path_to_top_parent := NULL]
    reis <- melt(reis, id.vars = c("rei_id", "most_detailed"), value.vars = parent_cols,
                 value.name = "parent_id", variable.name = "level")
    reis <- reis[!is.na(parent_id) & (most_detailed == 1 | rei_id == parent_id), ]
    reis[, rei_id := as.integer(rei_id)][, level := NULL]

    if (med) {
        # these are in draw space, so find the mean and drop the rest
        mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2017.csv")
        mediation[, mediation := rowMeans(.SD), .SDcols = paste0("draw_",0:999)]
        mediation <- mediation[, .(rei_id, cause_id, med_id, mediation)]
    }

    rei_collapse_vars <- c(id_vars[!id_vars %in% "rei_id"], "parent_id")
    rei_list <- reis[!(rei_id %in% unique(data$rei_id)), ]$rei_id %>% unique
    child_list <- reis[parent_id %in% rei_list & most_detailed == 1, ]$rei_id %>% unique
    data[, rei_id := as.integer(rei_id)]
    agg <- merge(data, reis[rei_id %in% child_list & parent_id %in% rei_list, ], by="rei_id", allow.cartesian=T)
    setkeyv(agg, rei_collapse_vars)

    # mediation
    if (med) {
        med_tmp <- mediation[rei_id %in% child_list & med_id %in% child_list, ]
        med_tmp <- merge(med_tmp, unique(agg[,.(rei_id,cause_id,measure_id,sex_id,age_group_id,most_detailed,parent_id)]),
                         by=c("rei_id","cause_id"), all.x=T, allow.cartesian = T)
        setnames(med_tmp, c("most_detailed","parent_id"),c("r_md","r_parent"))
        med_tmp <- merge(med_tmp, unique(agg[,.(rei_id,cause_id,measure_id,sex_id,age_group_id,most_detailed,parent_id)]),
                         by.x=c("med_id","cause_id","measure_id","sex_id","age_group_id"),
                         by.y=c("rei_id","cause_id","measure_id","sex_id","age_group_id"),
                         all.x=T, allow.cartesian = T)
        setnames(med_tmp, c("most_detailed","parent_id"),c("m_md","m_parent"))
        med_tmp <- med_tmp[m_parent == r_parent]
        setnames(med_tmp,"r_parent","parent_id")
        med_tmp <- med_tmp[, .(mediation = prod(1-mediation)),
                           by = c("rei_id", "cause_id", "parent_id", "measure_id", "sex_id", "age_group_id")]
        agg <- merge(agg, med_tmp, by = c("rei_id", "cause_id", "parent_id", "measure_id", "sex_id", "age_group_id"), all.x = T)
        agg[is.na(mediation), mediation := 1]
        agg[, paf := paf * mediation]
    }

    # aggregation
    agg <- agg[, .(paf = 1-prod(1-paf)), by = rei_collapse_vars]
    setnames(agg, "parent_id", "rei_id")
    data <- rbindlist(list(data, agg), use.names = T, fill = T)
    data[, rei_id := as.integer(rei_id)]
    return(data)
}

agg_cause <- function(data, gbd_round_id) {

    causes <- get_cause_metadata(cause_set_id = 3, gbd_round_id = gbd_round_id)
    data <- merge(data, causes[, .(cause_id, level, parent_id)], by = "cause_id")
    for (lvl in max(causes$level):(min(causes$level)+1)) {

        cdt <- data[level == lvl]
        pdt <- cdt[, .(risk_rate = sum(risk_rate)), by = eval(names(cdt)[!names(cdt) %in% c("risk_rate", "cause_id", "level")])]
        setnames(pdt, "parent_id", "cause_id")
        pdt <- merge(pdt, causes[, .(cause_id, parent_id, level)], by = "cause_id")
        data <- rbindlist(list(data, pdt), use.names = T, fill = T)

    }

    data[, c("level", "parent_id") := NULL]
    return(data)

}

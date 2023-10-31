read_mediation_matrix <- function() {
    mediation_file <- "FILEPATH/mediation_matrix_draw_gbd_2020.csv"
    mediation <- fread(mediation_file)
    return(mediation)
}

read_deltas <- function() {
    delta_file <- "FILEPATH/delta_draw_gbd_2020.csv"
    deltas <- fread(delta_file)
    return(deltas)
}

get_mediation <- function(risk_id) {
    mediation <- read_mediation_matrix()
    mediation <- melt(mediation, id.vars = c("rei_id", "med_id", "cause_id"),
                      measure.vars = paste0("draw_", 0:999), variable.name = "draw",
                      value.name = "mediation")
    mediation[, draw := as.numeric(gsub("draw_", "", draw))]
    mediation <- mediation[rei_id == risk_id,
                           list(mediation=1-prod(1-mediation)), by=c("cause_id", "draw")]
    mediation[, mean_mediation := mean(mediation), by="cause_id"]
    mediation <- mediation[mean_mediation != 1, ]
    mediation[, mean_mediation := NULL]
    return(mediation)
}

# Apply mediation to RR data set (long by draw).
# Returned dt will be empty if no mediation applies.
mediate_rr <- function(dt, mediation) {
    dt <- merge(dt, mediation, by = c("cause_id", "draw"))
    dt[, rr := (rr - 1) * (1 - mediation) + 1]
    dt[, mediation := NULL]
    return(dt)
}

# For a given risk, find all mediator risks in the mediation matrix
# and their deltas in the delta file.
get_mediator_cause_pairs <- function(risk_id) {
    mediators <- read_mediation_matrix()[rei_id == risk_id, ]
    data_cols <- c("mean_mediation", paste0("draw_", 0:999))
    if (any(is.na(mediators[, ..data_cols]))) {
        stop("Found mediation factors of NA for rei_id ", risk_id,
             ". Please submit a help desk ticket to Central Computation.")
    }
    mediators <- mediators[, .(rei_id, cause_id, med_id)]
    deltas <- read_deltas()[, .(rei_id, med_id, mean_delta)]
    mediators <- merge(mediators, deltas, by = c("rei_id", "med_id"), all.x = TRUE)
    return(mediators)
}

get_delta_draws <- function(risk_id) {
    deltas <- read_deltas()[rei_id == risk_id]
    deltas <- melt(deltas, id.vars = c("rei_id", "med_id"),
                   measure.vars = paste0("draw_", 0:999), variable.name = "draw",
                   value.name = "delta")
    deltas[, draw := as.numeric(gsub("draw_", "", draw))]
}

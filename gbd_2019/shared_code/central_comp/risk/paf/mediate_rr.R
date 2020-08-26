mediate_rr <- function(risk_id, dt) {
  mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2019.csv")
  mediation <- melt(mediation, id.vars = c("rei_id", "med_id", "cause_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "mediation")
  mediation[, draw := as.numeric(gsub("draw_", "", draw))]
  mediation <- mediation[mediation != 1 & rei_id == risk_id, ]
  mediation <- mediation[, list(mediation=1-prod(1-mediation)), by=c("cause_id", "draw")]
  if (nrow(mediation) == 0) stop("No mediation to apply")
  dt <- merge(dt, mediation, by=c("cause_id", "draw"))
  if (nrow(dt) == 0) stop("No valid mediated causes")
  dt[, rr := (rr-1)*(1-mediation) + 1]
  dt[, mediation := NULL]
  return(dt)
}

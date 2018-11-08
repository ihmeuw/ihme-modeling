## Summarize LT results -- get upper and lower from draw-level results
## id_vars should not include draw
gen_summary_lt <- function(lt, pop_dt, id_vars) {
  print(Sys.time())
  adult_age_ids <- c(8:16)
  child_age_ids <- c(28, 5)
  non_age_id_vars <- id_vars[id_vars != "age_group_id"]

  ## These aren't all the unique identifiers, just the draw ones that are required for age aggregation
  draw_id_vars <- c(non_age_id_vars, "draw")

  nn_lt <- lt[age_group_id %in% c(2:4)]
  lt <- lt[!age_group_id %in% c(2:4)]

  print(paste0("Draw Lifetable Starting ", Sys.time()))
  lt[, id := paste0(location_id, "_", draw)]
  lt <- lifetable(data = lt, preserve_u5 = 0, cap_qx = 1)
  lt[, id := NULL]

  setkeyv(lt, draw_id_vars)

  ## Save age 95-99 life table for mx approximation of 95+ in envelope process
  lt_95 <- lt[age_group_id == 33]
  lt[, px := 1 - qx]

  ## Generate 45q15
  lt_45q15 <- lt[age_group_id %in% adult_age_ids, list(qx = 1 - prod(px)), by = draw_id_vars]
  
  ## Generate 5q0
  lt_5q0 <- lt[age_group_id %in% child_age_ids, list(qx = 1 - prod(px)), by = draw_id_vars]

  ## Generate the lower/upper confidence intervals, as the mean will come from the mean lifetable
  lt_45q15 <- lt_45q15[, list(lower = lower(qx), upper = upper(qx)), by = non_age_id_vars]
  lt_45q15[, age_group_id := 199]
  lt_5q0 <- lt_5q0[, list(lower = lower(qx), upper = upper(qx)), by = non_age_id_vars]
  lt_5q0[, age_group_id := 1]

  lt_qx <- rbindlist(list(lt_5q0, lt_45q15))
  lt_qx[, lt_parameter := "qx"]

  ## Run the lifetable function on the draw-level data and then reshape long by lifetable parameter
  lt <- rbindlist(list(lt, nn_lt), use.names = T, fill = T)

  print(paste0("CI Starting ", Sys.time()))

  mean_mx_ax <- lt[, lapply(.SD, mean), .SDcols = c("mx", "ax", "qx"), by = c(id_vars)]

  lt <- melt(lt[, .SD, .SDcols = c(id_vars, "mx", "ax", "qx", "ex", "nLx")], id = c(id_vars))
  setnames(lt, "variable", "lt_parameter")
  lt <- lt[!is.na(value)] ## Drop NN results for ex and nLx

  id_vars_agg <- c(id_vars, "lt_parameter")
  setkeyv(lt, id_vars_agg)
  
  ci_lt <- lt[, list(lower = quantile(value, probs=.025, na.rm=T), 
                  upper = quantile(value, probs=.975, na.rm=T)), 
            by = id_vars_agg]

  ci_lt <- rbindlist(list(ci_lt, lt_qx), use.names = T)

  print(paste0("Mean LT ", Sys.time()))
  nn_mx_ax <- mean_mx_ax[age_group_id %in% c(2:4)]
  mean_mx_ax <- mean_mx_ax[!age_group_id %in% c(2:4)]

  mean_mx_ax[, id := location_id]
  mean_lt <- lifetable(data = mean_mx_ax, preserve_u5 = 0, cap_qx = 1)
  mean_lt[, id := NULL]

  mean_nn <- rescale_nn_qx_mx(mean_mx_ax[age_group_id == 28],
                              nn_mx_ax,
                              id_vars = c("location_id", "year_id", "sex_id"))

  ## Generate 5q0 and 45q15 from the mean lifetable
  lt_45q15 <- mean_lt[age_group_id %in% adult_age_ids, list(qx = 1 - prod(px)), by = non_age_id_vars]
  lt_45q15[, age_group_id := 199]
  lt_5q0 <- mean_lt[age_group_id %in% child_age_ids, list(qx = 1 - prod(px)), by = non_age_id_vars]
  lt_5q0[, age_group_id := 1]

  mean_lt <- rbindlist(list(mean_lt, mean_nn), use.names = T, fill = T)
  mean_lt <- melt(mean_lt[, .SD, .SDcols = c(id_vars, "mx", "ax", "qx", "ex", "nLx")], id = c(id_vars))
  setnames(mean_lt, "variable", "lt_parameter")
  mean_lt <- mean_lt[!is.na(value)] ## Drop NN results for ex and nLx
  setnames(mean_lt, "value", "mean")

  mean_lt_qx <- rbindlist(list(lt_45q15, lt_5q0))
  mean_lt_qx[, lt_parameter := "qx"]
  setnames(mean_lt_qx, "qx", "mean")
  
  ## Combine datasets
  mean_lt <- rbindlist(list(mean_lt_qx, mean_lt), use.names = T)

  summary_lt <- merge(mean_lt, ci_lt, by = c(id_vars_agg))
  return(list(summary_lt, lt_95))
}
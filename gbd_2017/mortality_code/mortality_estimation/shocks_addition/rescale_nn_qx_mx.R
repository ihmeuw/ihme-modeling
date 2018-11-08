rescale_nn_qx_mx <- function(u1_dt, nn_dt, id_vars) {
  ## Input: data.table with age_group_ids 2-4 and 28
  ## Purpose: Aggregate u1 neonatal qx, compare to estimated 1q0, and then rescale NN probabilities to align
  ## Then, recalculate NN mx etc. appropriately
  u1_dt[, c("mx", "ax", "age_group_id") := NULL]
  setnames(u1_dt, "qx", "u1_qx_target")

  setkeyv(nn_dt, id_vars)
  nn_dt[age_group_id == 2, t := 7/365]
  nn_dt[age_group_id == 3, t := 21/365]
  nn_dt[age_group_id == 4, t := (365-21-7)/365]

  nn_dt[, qx := mx_to_qx(mx, t)]
  nn_dt[, px := 1 - qx]
  setorderv(nn_dt, c(id_vars, "age_group_id"))
  nn_dt[, qx_nn_u1 := 1 - prod(px), by = id_vars]

  ## Here we want to get the conditional probability of survival up to the interval multiplied by the qx of the interval, and then divided by the overall qx
  ## This represents the conditional mortality, summing up to 1 -- we can then use this to resplit the under-1 qx values appropriately
  nn_dt[, cond_prob := qx * cumprod(px) / (px * qx_nn_u1), by = id_vars]
  nn_dt <- merge(nn_dt, u1_dt, by = id_vars)
  nn_dt[, qx := u1_qx_target * cond_prob / (cumprod(px) / px), by = id_vars]
  nn_dt[, mx := qx_to_mx(qx, t = t)]
  nn_dt[, ax := mx_qx_to_ax(m = mx, q = qx, t = t)]

  nn_dt <- nn_dt[, .SD, .SDcols = c(id_vars, "age_group_id", "mx", "ax", "qx")]
  return(nn_dt)
}

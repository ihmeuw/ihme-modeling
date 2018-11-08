###########################################################
### Project: HSA (maternal)
### Purpose: Multiply ratio
###########################################################


###########################################################################################################################
# Multiply out draws of ratios for ANC4/ANC1
###########################################################################################################################

prep.ratio <- function(me, head, draws=FALSE, path=save.root, dupe=FALSE, flip=FALSE) {
  ## SETUP AND LOAD
  key <- c("location_id", "year_id", "age_group_id", "sex_id")
  mes <- unlist(strsplit(me, "_"))[2:3]
  num <- paste0(head, mes[1])
  denom <- paste0(head, mes[2])
  ## IF FLIP
  if (flip) {
    num <- paste0(head, mes[2])
    denom <- paste0(head, mes[1])
  } 
  id.ratio <- best.run_id(me)
  id.denom <- best.run_id(denom)
  df.ratio <- read.draws(id.ratio)
  df.ratio <- set.intro(df.ratio, me)
  df.denom <- read.draws(id.denom)
  df.ratio <- cap.est(df.ratio)
  df.denom <- cap.est(df.denom)
  ## RESHAPE LONG AND MERGE
  df.ratio <- melt(df.ratio, id.vars=key, measure=patterns("^draw"), variable.name="draw",  value.name="ratio")
  df.denom <- melt(df.denom, id.vars=key, measure=patterns("^draw"), variable.name="draw", value.name="denom")
  df <- merge(df.ratio, df.denom, by=c(key, "draw"))
  ## MULTIPLY OUT RATIO 
  if (flip) df <- df[, ratio := 1/ratio]
  df <- df[, est := ratio * denom]
  df <- df[, c("ratio", "denom") := NULL]
  df <- df[est >=1, est := 0.99]
  ## IF DRAWS, RESHAPE
  if (draws) {
    df <- dcast(df, location_id + year_id + age_group_id + sex_id ~ draw, value.var="est")
    df <- df[,measure_id := 18]
    if (dupe) {
      if (me != "vacc_pcv3_dpt3_ratio") df <- duplicate.draws(df)
      if (me == "vacc_pcv3_dpt3_ratio") df <- duplicate.draws(df, age_group_ids=c(2:20, 30, 31, 32, 235))
    }
    save.draws(df, num, path)
  } else {
    ## ELSE CALCULATE SUMMARY
    df <- df[, gpr_mean := mean(est), by=key]
    df <- df[, gpr_lower := quantile(est, 0.025), by=key]
    df <- df[, gpr_upper := quantile(est, 0.975), by=key]
    df <- df[, c(key, "gpr_mean", "gpr_lower", "gpr_upper"), with=F] %>% unique
    ## Save Collapsed
    df <- df[, me_name := num]
    save.collapsed(df, num)
  }
  print(paste0("Saved collapsed ", num, " under run_id ", id.ratio))
}



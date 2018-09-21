rm(list = ls())
library(data.table)

optimized_log <- function(x) {
  ### ### ### ### ### ### ### ### ### ### ### ###
  ### adapted from https://rdrr.io/cran/cwhmisc/man/f.log.html
  ### function to determine offset for indicators w/ 0s present
  # x <- dt[indicator_id == 1019]$draw_0 # test with a disaster draw
  # fx <- optimized_log(x)
  # ## Not run:
  # oldpar <- par(mfrow = c(2, 3))
  # plot(x,main="exp(normal)+zeros")
  # qqnorm(x)
  # T3plot(x) # requires library "cwhmisc"
  # plot(fx,main="optimized offset")
  # qqnorm(fx)
  # T3plot(fx) # requires library "cwhmisc"
  # par(oldpar)
  ### ### ### ### ### ### ### ### ### ### ### ###
  if (length(x[x == 0]) > 0) {
    aus <- x[x > 0]
    const <- median(aus) / ((median(aus) / quantile(aus, probs = 0.25)) ^ 2.9)
    log(x + const)
  } else log(x)
}

scale.vector <- function(X, scale) {
  # Make return vector (use optimized log value to scale infinites)
  X[X < 1.1e-10] <- 0
  if (unique(scale) == "infinite") Y <- optimized_log(X)
  else Y <- X

  # Use percentiles
  Y <- (Y - quantile(Y, 0.025)) / (quantile(Y, 0.975) - quantile(Y, 0.025))
  Y[Y < 0] <- 0
  Y[Y > 1] <- 1
  return(Y)
}


scale.dt <- function(dt, draws, max_draw=999) {
  # scale each column in the data table
  print("Scaling draws")
  setkey(dt, indicator_id)
  if (draws) {
    scale.cols <- paste0("draw_", 0:max_draw)
  } else {
    scale.cols <- c("mean_val", "upper", "lower")
  }
  for (col in scale.cols) {
    if (grepl("00", col)) print(col)
    dt[, (col):=scale.vector(get(col), scale), by=indicator_id]
    dt[[col]][dt$invert == 1] <- 1 - dt[[col]][dt$invert == 1]
    if (nrow(dt[is.na(get(col))]) > 0) stop("NaNs induced in transtormation (", col, ")")
  }
  dt[, c("scale", "invert") := NULL]
  return(dt)
}


get.indic.table <- function() {
  # get the indicator table
  indic_table <- fread("FILEPATH")
  return(indic_table)
}


read.dt <- function(sdg_version, draws, indic_table, max_draw=999) {
  # read in a data table with values to scale
  print(paste0("Reading in data (version ", sdg_version, ")"))
  if (!draws) {
    path = "FILEPATH"
    dt0 <- fread(path)
    it <- subset(indic_table, select = c("indicator_id", "indicator_target", "scale", "invert"))
    dt0 <- merge(dt0, it, by=c("indicator_id"), all.x = TRUE)

    cols <- c(c("indicator_id", "location_id", "year_id", "indicator_target", "scale", "indvert"), c("mean_val", "upper", "lower"))
    dt0 <- subset(dt0, select=cols)
  } else {
    path = "FILEPATH"
    dt0 <- fread(path)
    it <- subset(indic_table, select = c("indicator_id", "indicator_target"))
    dt0 <- merge(dt0, it, by=c("indicator_id"), all.x = TRUE)
    cols <- c(c("indicator_id", "location_id", "year_id", "indicator_target", "scale", "invert"), paste0("draw_", 0:max_draw))
    dt0 <- subset(dt0, select=cols)
  }

  return(dt0)
}


calc.composite.index <- function(dt, ids_in_comp, indic_id, draws, max_draw=999) {
  # calculate a composite index over each column in the datatable and append
  idx_dt <- copy(dt)
  idx_dt <- idx_dt[idx_dt$indicator_id %in% ids_in_comp]
  idx_dt[, c("indicator_id") := NULL]

  geom.mean <- function(X) {
    return(exp(sum(log(X)) / length(X)))
  }

  if (draws) {
    scale.cols <- paste0("draw_", 0:max_draw)
  } else {
    scale.cols <- c("mean_val", "upper", "lower")
  }

  for (col in scale.cols) {
    idx_dt[[col]][idx_dt[[col]] < 0.01] <- .01
  }
  print("Geom by target first")
  # print("Mean by target first")
  idx_dt <- idx_dt[, lapply(.SD, geom.mean), by=c('location_id', 'year_id', 'indicator_target'), .SDcols=scale.cols]
  # idx_dt <- idx_dt[, lapply(.SD, mean), by=c('location_id', 'year_id', 'indicator_target'), .SDcols=scale.cols]
  idx_dt[, c("indicator_target") := NULL]

  print("Then geometric mean of those")
  # print("Then mean of those")
  idx_dt <- idx_dt[, lapply(.SD, geom.mean), by=c('location_id', 'year_id'), .SDcols=scale.cols]
  # idx_dt <- idx_dt[, lapply(.SD, mean), by=c('location_id', 'year_id'), .SDcols=scale.cols]
  idx_dt[, c("indicator_id") := indic_id]

  return(idx_dt)
}


compile.dt <- function(dt, draws, indic_table, sdg_version, max_draw=999) {
  # calculate indices and collapse means together

  sdg_ids <- unique(indic_table[indic_table$indicator_status_id == 1]$indicator_id)
  mdg_ids <- unique(indic_table[indic_table$indicator_status_id == 1 & indic_table$mdg_agenda==1]$indicator_id)
  nonmdg_ids <- unique(indic_table[indic_table$indicator_status_id == 1 & indic_table$mdg_agenda==0]$indicator_id)

  print("SDG Index")
  sdg_dt <- calc.composite.index(dt, sdg_ids, 1054, draws, max_draw=max_draw)
  print("MDG Index")
  mdg_dt <- calc.composite.index(dt, mdg_ids, 1055, draws, max_draw=max_draw)
  print("NON MDG Index")
  nonmdg_dt <- calc.composite.index(dt, nonmdg_ids, 1060, draws, max_draw=max_draw)

  dt[, c("indicator_target") := NULL]
  dt <- rbind(dt, sdg_dt)
  dt <- rbind(dt, mdg_dt)
  dt <- rbind(dt, nonmdg_dt)

  if (draws) {
    print("Saving draws")
    write.csv(dt, "FILEPATH", row.names=FALSE)
  }

  return(dt)
}


draws = TRUE
sdg_version = 27
max_draw = 999
indic_table <- get.indic.table()
dt <- read.dt(sdg_version, draws, indic_table, max_draw=max_draw)
dt <- scale.dt(dt, draws, max_draw=max_draw)
dt <- compile.dt(dt, draws, indic_table, sdg_version, max_draw=max_draw)

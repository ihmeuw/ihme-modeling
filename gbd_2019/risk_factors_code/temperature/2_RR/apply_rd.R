# Summary: This function takes a cleaned and formatted VR dataset and...
# 1) seperates the ICD codes that mapped to GBD causes from those that map to garbage;
# 2) loops over all years and locations (most-detailed GBD location) of VR data with garbage codes;
# 3) merges in proportional redistribution files, and applies the redistribution proportions;
# 4) combines mapped and redistributed data;
# 5) reshapes the data from long to wide, to produce one row for every location, date, age, and sex, and one column for each cause of death;
# 6) expands out the dataset to ensure that there is one row for every location, date, age, and sex (i.e. it fills in any rows where no deaths occurred);
# 7) calculates all cause mortality, and drops and causes with fewer than 1 death (since these are not useful for our analysis)
#
# Requires: dplyr, data.table (should have already been loaded by the master script)



apply_rd <- function(dt, paths, iso3) {

  ac <- fread(paths$acPath) %>% dplyr::select(cause_id, ends_with("3"))

  done <- copy(dt)
  done <- done[cause_id!=743,][, n_deaths := 1]
  done <- merge(done, ac, by = "cause_id", all.x = T)[, .(location_id, zonecode, year, date, sex, age, acause_3, n_deaths)]


  tp <- copy(dt)
  tp <- tp[cause_id==743,][, cause_id := NULL]

  cat("Applying redistribution to garbage coded deaths by year and province...", "\n")

  rd <- data.table()
  spacer <- ""
  for (y in unique(tp[, year])) {

    loc_ids <- unique(tp[year==y, location_id])

    line <- (nchar(paste(loc_ids, collapse = ".")) - 6) / 2
    if (line<=1) line <- 1

    cat(paste(rep("-", floor(line)), collapse = ""), y, paste(rep("-", ceiling(line)), collapse = ""), "\n")

    for (loc_id in loc_ids) {

      cat(paste0(spacer, loc_id))

      ytemp <- copy(tp)
      ytemp <- ytemp[year==y & location_id==loc_id, ]

      rdmap <- data.table(readRDS(paste0(paths$rdPath, iso3, "_", loc_id, "_", y, "_rdmap.rds")))

      ytemp <- merge(ytemp, rdmap, by.x = c("value", "age", "sex", "year"), by.y = c("gc_in", "age", "sex", "year"), all.x = T)
      ytemp[is.na(cause_id)==T,  `:=` (cause_id = 743, weights = 1)]
      ytemp[is.na(weights)==T, weights := 1]
      ytemp <- merge(ytemp, ac, by = "cause_id", all.x = T)[, .(location_id, zonecode, year, date, sex, age, acause_3, weights)]
      ytemp <- ytemp[, sum(weights), by = .(location_id, zonecode, year, date, sex, age, acause_3)]
      names(ytemp)[grep('^V1', names(ytemp))] <- 'n_deaths'

      rd <- rbind(rd, ytemp, fill = T, use.names = T)
      spacer <- "."

    }
    
  cat("\n")  

  }

  # append non-gc rows to redistributed gc rows
  cat("\n", "Appending redistributed rows", "\n")
  final <- rbind(done, rd, use.names=TRUE, fill = TRUE)

  # collapse to sum of deaths by location, date, age, sex, and level-3 cause
  cat("Collapsing sum of deaths by location, date, age, sex, and level 3 cause", "\n")
  final <- final[, sum(n_deaths), by = .(location_id, zonecode, year, date, age, sex, acause_3)]
  names(final)[grep('^V1', names(final))] <- 'n'

  # reshape cause-specific deaths to wide by age, location, and date
  cat("Reshaping to wide by cause", "\n")
  final <- dcast(final, location_id + zonecode + year + date + sex + age ~ paste0('n_', acause_3), value.var = 'n')

  # create a data frame with every possible combination of location, age, and date
  cat("Creating master with every possible combination of location, age, and date", "\n")
  full <- merge(data.table(unique(final[, c("zonecode", "location_id")]), merge = 1), data.table(date = as.Date(min(final$date):max(final$date), origin = "1970-01-01"), merge = 1), by = "merge", all = T, allow.cartesian = T)
  full <- merge(full, data.table(age = unique(final$age), merge = 1), by = "merge", all = T, allow.cartesian = T)
  full <- merge(full, data.table(sex = unique(final$sex), merge = 1), by = "merge", all = T, allow.cartesian = T)[, merge := NULL]
  full[, year := as.integer(format(date, "%Y"))]

  cat("Merging CoD data to master", "\n")
  full <- merge(full, final, by = c('zonecode', 'location_id', 'year', 'date', 'age', 'sex'), all = T)

  causeList <- grep("^n_", names(full), value = T)
  full[, paste0(causeList) := lapply(.SD, function(x) {replace_na(x, 0)}), .SDcols = causeList]
  full[, 'n_all_cause' :=  rowSums(.SD, na.rm = T), .SDcols = causeList]

  # DETERMINE NUMBER OF DEATHS BY CAUSE AND REMOVE ALL CAUSES FOR WHICH THERE IS <1 DEATH IN THE DATASET
  # (these contribute no information and their removal reduces computational overhead)
  cat("Removing causes with fewer than one death", "\n")
  totals <- full[, sapply(.SD, function(x) {sum(x, na.rm = TRUE)}), .SDcols= causeList]
  full[, names(totals)[totals<1] := NULL]

  cat("Done", "\n", "\n")

  return(full)
}

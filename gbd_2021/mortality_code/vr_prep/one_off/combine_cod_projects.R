
# TITLE: Combine CoD versions to fill missing locations

library(data.table)

new_run_id <- 

# Load -------------------------------------------------------------------------

cod_14 <- fread(paste0())

cod_15 <- fread(paste0())

# Combine ----------------------------------------------------------------------

# manual list of expected nids based on diagnostics
missing_nids <- c(497759, 496216, 496217, 496218, 494874, 494875, 494876, 501980)

# check that nids missing from 14 and present in 15
for (n in missing_nids){
  assertthat::assert_that(!n %in% unique(cod_14$nid))
  assertthat::assert_that(n %in% unique(cod_15$nid))
}

# keep only missing nids 
cod_15 <- cod_15[nid %in% missing_nids]

# drop duplicate source from old cod 
cod_14 <- cod_14[!(nid == 464292 & location_id == 69)]
  
# combine 
cod <- rbind(cod_14, cod_15)

# check that nids now present in cod
for (n in missing_nids){
  assertthat::assert_that(n %in% unique(cod$nid))
}

# check for duplicates 
cod_IDcols <- setdiff(names(cod), c("deaths","nid","source","underlying_nid"))
cod_dedup <- copy(cod)
cod_dedup[, dup := .N, by = cod_IDcols]
assertable::assert_values(cod_dedup, "dup", test="equal", test_val = 1)

# save 
readr::write_csv(cod, paste0())


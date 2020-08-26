# Summary: This function takes a raw VR dataset and...
# 1) cleans it to retain only needed variables, standardize variable names and formats,
# 2) produce derivative variables (e.g. produce a year variable from the date variable),
# 3) link official administrative codes (zonecode) to GBD location IDs (at the province level),
# 4) map ICD 10 codes to GBD causes
#
# Requires: dplyr, data.table (should have already been loaded by the master script)
#



clean_vr = function(df, paths, varnames, iso3) {

  setDT(df)

  # keep necessary variables, and standardize variable names
  df <- df[, c(as.character(varnames)), with = F]
  names(df) <- names(varnames)


  # bring in link, age/sex restriction, and mapping files
  cat("Loading link, ICD restriction and mapping files...")
  icdmap   <- fread(paths$icdmapPath)
  restrict <- fread(paths$restrictPath)[, acause := NULL]
  cat("Done", "\n")


  # ensure that zonecode and sex are coded as integers
  df[, c('zonecode', 'sex') := lapply(.SD, function(x) {type.convert(as.character(x))}), .SDcols = c('zonecode', 'sex')]


  # zonecode
  # Filter for valid location codes
  if (iso3=="CHN") {
    cat("Formatting locations...")

    df <- df[between(zonecode,10**7,10**8),]
    
    plink <- fread(paths$plinkPath) %>% dplyr::select(province, location_id)
    
    # Truncate zone code to desired admin level(for admin2 level, set to df.zonecode %/% 100)
    df[, province := zonecode %/% 1000000L]
    df[, zonecode := zonecode %/% 100L]

    # merge in province link file to determine the ihme location id for each zonecode
    df <- merge(df, plink, by = 'province', all.x = TRUE)

    cat("Done", "\n")
  }



  # date
  cat("Formatting dates...")
  df[, date := as.Date(date)]
  df[, year := as.integer(format(date, "%Y"))]
  cat("Done", "\n")

  # drop if missing any essential variables
  df <- df[!is.na(df$zonecode) & !is.na(df$value) & !is.na(df$date) & !is.na(df$location_id),]
  
  
  # age stratify for redistribution
  cat("Formatting ages...")

  df = df[!is.na(df$age),]
  convert_age = function(age){
    if (age > 95){return(95)}
    if (age >= 5){return((age %/% 5) * 5)}
    if (age < .01){return(0)}
    if (age < .1){return(.01)}
    if (age < 1){return(.1)}
    return(1)
  }

  df[, age := sapply(df$age,convert_age)]

  cat("Done", "\n")



  # SETUP FOR REDISTRIBUTION -----------------------------------------------------------------

  # merge in icd map and determine which icd codes link to gbd causes
  cat("Merging in ICD map and applying age/sex restrictions...")
  big <- merge(df, icdmap, by = 'value', all.x = TRUE)

  # where icd codes failed to link to gbd cause, remove decimal precision and re-merge to icd map
  big[big$cause_id==743 | is.na(big$cause_id)==TRUE, value := gsub("\\..*$", "", value)]
  big <- big[, 'cause_id' := NULL]
  big <- merge(big, icdmap, by = 'value', all.x = TRUE)

  # where codes failed the second merge, code them as garbage
  big[is.na(cause_id), cause_id := 743]

  # apply age-sex restrcitions
  big <- merge(big, restrict, by = 'cause_id', all.x = T)

  big[, ok := age_end>=age]
  big[, ok := ok * (age_start<=age)]
  big[sex==1, ok := ok * male]
  big[sex==2, ok := ok * female]

  big[ok==F, `:=` (value = "ZZZ", cause_id = 743)]

  big[, c("female", "male", "age_end", "age_start", "ok") := NULL]
  cat("Done", "\n", "\n")


  return(big)
}


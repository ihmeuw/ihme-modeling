# Recall bias correction and regression

rm(list = ls())
library(readr)
library(data.table)
library(mortdb)
library(haven)

user <- "USERNAME"
if (Sys.info()[1] == "Windows") {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
}

gbd_year <- 2023

# Read in inputs
siblingdir <- "FILEPATH"
file_list <- Sys.glob(fs::path(siblingdir, "/FILEPATH/fullmodel_svy.csv"))
## add snnp subnats
subnat_list <- Sys.glob(paste0(siblingdir, "/FILEPATH/fullmodel_svy.csv"))
file_list <- c(file_list, subnat_list)

master_df <- NULL
for (f in file_list) {
  infile <- fread(f)
  master_df <- rbind(master_df, infile)
}
master_df <- master_df[svy != "BDI_2016_2017"]
exist_svy <- master_df[, svy] |> unique()

## Read in already dataset from previous years
old_data <- fread("FILEPATH/compiled_data_2024-06-25.csv")

## Remove redundant records for UGA and TLS
old_data <- old_data[!svy %in% exist_svy]

bind_cols <- c("svy", "female", "period", "q45q15", "NID", "deaths_source")
master_df <- rbind(old_data[, ..bind_cols], master_df)

### check for duplicates
dup_check <- unique(master_df[, .(svy)])
dup_check[, svy_interview := gsub("DHS_", "", svy)]
dup_check[, multi_year := grepl("_\\d{4}_\\d{4}$", svy_interview)]
dup_check[(multi_year), svy_interview := gsub("_\\d{4}$", "", svy_interview)]
dup_svy <- dup_check[order(svy_interview)] |>
  p(svy_interview == shift(svy_interview, type = "lag"), svy)
cat(paste0(
  "Duplicated surveys include: ",
  paste(dup_svy, collapse = ", "),
  "\n"
))
flush.console()

# save new records
master_df <- master_df[!svy %in% dup_svy]
date_format <- format(Sys.Date(), "%Y-%m-%d")
master_df |> fwrite(glue::glue("FILEPATH/compiled_data_{date_format}.csv"))

## Redundant inputs
input <- master_df[deaths_source != ""]
input[, svy := gsub("DHS_", "", svy)]

# Survey information -----------------------------------------------------

input[, year_start := regexpr("_(2|1)", svy)]
input[, year := as.integer(substr(svy, year_start + 1, year_start + 4))]
input[svy != "IRQ_2007", year := year - 1L]
input[, year_start := NULL]

input[, year := (year - period) + 1.5] # So that the first period starts in 2013.5 if the survey was in 2014

# identify ihme_loc_id
input[, ihme_loc_id := gsub("_.*$", "", svy)]

# Drop locations that aren't estimated
input <- input[!(ihme_loc_id %in% c("AP02009", "AP12006", "Bohol02009", "Bohol12006", "Pemba12006", "UP12007")), ]

# subnational ihme_loc_ids get an underscore
input[
  nchar(ihme_loc_id) > 3,
  ihme_loc_id := gsub("^([A-Z]{3})([0-9]+)$", "\\1_\\2", ihme_loc_id)
]

input[, sex := ifelse(female == 1, "female", "male")]

# merge onto locations
locs <- get_locations(gbd_year = gbd_year)
locs <- locs[, .(ihme_loc_id, location_name, location_id)]
alldata <- merge(input, locs, by = "ihme_loc_id", all.x = T)

# generate duplicates for country year sex

alldata[, dup := 1:.N, by = c("ihme_loc_id", "year", "sex")]
keep_cols <- gsub("female", "sex", bind_cols)
keep_cols <- gsub("period", "year", keep_cols)
alldata <- alldata[, ..bind_cols]
alldata[, suryear := as.integer(substr(svy, nchar(svy) - 3, nchar(svy)))]

# Drop subnationals
subnats <- alldata[grepl(
  "BRA[0-9]+|ETH_[0-9]{5}|IDN[0-9]+|KEN_[0-9]{5}|ZAF[0-9]{3}",
  svy
)]
alldata <- alldata[!(svy %in% unique(subnats$svy))]

# Pair surveys: similar locations with overlapping measurements
paired <- NULL
keep_cols <- gsub("NID", "ihme_loc_id", keep_cols)
keep_cols <- gsub("deaths_source", "suryear", keep_cols)
for (v in unique(alldata$svy)) {
  subs <- alldata[svy == v, ..keep_cols]
  setnames(
    subs,
    c("svy", "q45q15", "suryear"),
    c("basesvy", "base45q15", "base_suryear")
  )
  to_add <- merge(subs, alldata, by = c("ihme_loc_id", "year", "sex"))

  setnames(
    to_add,
    c("svy", "q45q15", "suryear"),
    c("compsvy", "comp45q15", "comp_suryear")
  )
  to_add <- to_add[base_suryear > comp_suryear, ]
  paired[[v]] <- to_add
}

paired <- do.call("rbind", paired)
paired[, diff := comp45q15 - base45q15]
paired[, year_diff := as.integer(base_suryear) - as.integer(comp_suryear)]


# calculate the log difference
paired[, lnbase45q15 := log(base45q15)]
paired[, lncomp45q15 := log(comp45q15)]
paired[, lndiff := lncomp45q15 - lnbase45q15]

# Output pairs
fwrite(paired, paste0("FILEPATH/paired_data_", format(Sys.Date(), "%d %b %Y"), ".csv"))

# Run the regression separately by sex
male_model <- lm(formula = diff ~ year_diff + 0, data = paired[sex == "male", ])
fem_model <- lm(formula = diff ~ year_diff + 0, data = paired[sex == "female", ])

reg_results <- data.table(
  "coeffs" = c(male_model$coefficients, fem_model$coefficients),
  "se" = c(coef(summary(male_model))[, "Std. Error"], coef(summary(fem_model))[, "Std. Error"]),
  "sex" = c(1, 2)
)
reg_results[, `:=`(lb45q15 = coeffs - 1.96 * se, ub45q15 = coeffs + 1.96 * se)]

fwrite(reg_results, paste0("FILEPATH/reg_coefficients_", format(Sys.Date(), "%d %b %Y"), ".csv"))

# Apply recall bias correction to estimates --------------------------------

adj_results <- copy(input)
adj_results[, c("adj45q15", "adj45q15_lower", "adj45q15_upper") := q45q15]

# Apply regression coefficients arithmetically
for (s in c(1, 2)) {
  gender <- ifelse(s == 1, "male", "female")
  est <- reg_results[sex == s, c("sex", "coeffs", "lb45q15", "ub45q15")]
  for (i in seq(1, 15)) {
    adj_results[period == i & sex == gender, `:=`(
      adj45q15 = adj45q15 + est$coeffs * i,
      adj45q15_lower = adj45q15_lower + est$lb45q15 * i,
      adj45q15_upper = adj45q15_upper + est$ub45q15 * i
    )]
  }
}
# simplify if greater than 1
adj_results[adj45q15 > 1, adj45q15 := 1]
adj_results[adj45q15_upper > 1, adj45q15_upper := 1]
adj_results[adj45q15_lower > 1, adj45q15_lower := 1]


# Final configuration -----------------------------------------------------

# Add a NID map
nid_map <- setDT(readr::read_csv("FILEPATH/nid_map_gbd2016.csv"))
nid_map[, svy_type := NULL]

est_global_sib <- merge(adj_results, nid_map, by = "svy", all.x = T)
est_global_sib[, NID := as.integer(NID)]
est_global_sib[is.na(nid), nid := NID]
assertable::assert_values(est_global_sib, "nid", test = "not_na")

# Subset to necessary cols
est_global_sib <- est_global_sib[, .(svy, deaths_source, year, ihme_loc_id, sex, adj45q15, adj45q15_lower, adj45q15_upper, nid)]

# Generate sourcedate
est_global_sib[, source_date := as.integer(regmatches(svy, regexpr(".{4}$", svy)))]

# Split of PER sources assigned to same nid
est_global_sib[ihme_loc_id == "PER" & source_date == 2003, nid := 275090]
est_global_sib[ihme_loc_id == "PER" & source_date == 2009, nid := 270404]

# Set ihme_loc_id
subnat_ihmes <- "BRA[0-9]+|ETH_[0-9]{5}|IDN[0-9]+|KEN_[0-9]{5}|ZAF[0-9]{3}"
est_global_sib[grepl(subnat_ihmes, svy), ihme_loc_id := regmatches(svy, regexpr(subnat_ihmes, svy))] # Extract the IHME loc ID from svy
est_global_sib[(substr(ihme_loc_id, 4, 4) != "_") & (nchar(ihme_loc_id) > 3), ihme_loc_id := paste0(substr(ihme_loc_id, 1, 3), "_", substr(ihme_loc_id, 4, 1000))] # Add in underscore if not present for formatting

# Assert it's all correct
if (nrow(est_global_sib[is.na(ihme_loc_id)]) > 0) {
  print(est_global_sib[is.na(ihme_loc_id)])
  stop("Above rows missing ihme_loc_id")
}

if (any(!(est_global_sib$ihme_loc_id %in% locs$ihme_loc_id))) {
  extra_locs <- setdiff(est_global_sib$ihme_loc_id, locs$ihme_loc_id)
  print(extra_locs)
  stop("Unknown IHME loc ID")
}

# write output
fwrite(est_global_sib, paste0("FILEPATH/recall_bias_output_", format(Sys.Date(), "%d %b %Y"), ".csv"))

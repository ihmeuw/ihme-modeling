# clear workspace
rm(list = ls())

# list files
data_lists <- list.files('<<<< FILEPATH REDACTED >>>>>/Africa outputs - 17-22.03.2017/')

# load packages
require(plyr)
require(foreign)
require(readstata13)

# loop through and rbind files
combined <- NULL

for(i in 1:length(data_lists)){

  # get file path
  file_n <- data_lists[i]

  # generate path
  path <- paste('<<<< FILEPATH REDACTED >>>>>/Africa outputs - 17-22.03.2017/', file_n, sep = "")

  # read in file
  temp <- read.dta13(path, convert.factors = FALSE)

  # bind files
  if(i == 1){

    combined <- temp

  } else {

    combined <- rbind.fill(combined,
                           temp)

  }

}

# processing
# set mothers age group
combined$mothers_age_group <- NULL
combined$mothers_age_group[combined$mothers_age >44 & combined$mothers_age <50] <- "45-49"
combined$mothers_age_group[combined$mothers_age >39 & combined$mothers_age <45] <- "40-44"
combined$mothers_age_group[combined$mothers_age >34 & combined$mothers_age <40] <- "35-39"
combined$mothers_age_group[combined$mothers_age >29 & combined$mothers_age <35] <- "30-34"
combined$mothers_age_group[combined$mothers_age >24 & combined$mothers_age <30] <- "25-29"
combined$mothers_age_group[combined$mothers_age >19 & combined$mothers_age <25] <- "20-24"
combined$mothers_age_group[combined$mothers_age >14 & combined$mothers_age <20] <- "15-19"

# drop mothers not within fertile age
# subset to those records without a mothers age
combined <- combined[combined$mothers_age >=15, ]
combined <- combined[combined$mothers_age <= 49, ]

# label child alive variable
combined$child_alive[combined$child_alive == 0] <- "No"
combined$child_alive[combined$child_alive == 1] <- "Yes"

combined2 <- combined[!is.na(combined$mothers_age), ]

####### few checks on the datasets #######
combined_dup <- combined2
combined_dup$child_age_at_death_months <- as.numeric(combined_dup$child_age_at_death_months)
combined_dup <- combined_dup[combined_dup$year >= 1998 & combined_dup$child_age_at_death_months <= 300 & combined_dup$child_age_at_death_months >= 0, ]
surveys <- sort(unique(combined_dup$nid))

pdf('<<<< FILEPATH REDACTED >>>>>/Africa CBH_SBH - CBH aod distributions.pdf',
    width = 8.27,
    height = 11.29)
par(mfrow = c(4, 3))

br <- seq(0, 300, by = 10)
for (s in seq_along(surveys)) {
  sub <- combined_dup[which(combined_dup$nid == surveys[s]), ]
  # set title for the plot
  title <- paste(unique(sub$country), unique(sub$year), unique(sub$nid), sep = " ")

  hist(sub$child_age_at_death_months,
       xlim = c(0, 300),
       col = grey(0.4),
       border = 'white',
       breaks = br,
       xlab = 'age at death (months)',
       main = title)
}

dev.off()

### part 2: create frequency distributions for the sbh variables
### plot 1) children ever born data
combined_dup <- combined2

# change children ever born and children ever dead fields to numeric
combined_dup$ceb <- as.numeric(combined_dup$ceb)
combined_dup$ced <- as.numeric(combined_dup$ced)

# limit data to records within data range of interest
combined_dup <- combined_dup[combined_dup$year >= 1998, ]

# create a unique list of surveys based on NID
surveys <- sort(unique(combined_dup$nid))

# define name of plot (children ever born)
pdf('<<<< FILEPATH REDACTED >>>>>/Africa CBH_SBH - SBH ceb.pdf',
    width = 8.27,
    height = 11.29)
par(mfrow = c(3, 3))

br <- seq(0, 30, by = 1)
for (s in seq_along(surveys)) {
  sub <- combined_dup[combined_dup$nid == surveys[s], ]
  # make one record per mother
  sub <- unique(sub[c('nid', 'country', 'year', 'mid', 'ceb')])
  # remove mothers where number of children born is unknown
  sub <- sub[!is.na(sub$ceb), ]
  # set title for the plot
  title <- paste(unique(sub$country), unique(sub$year), unique(sub$nid), sep = " ")

  hist(sub$ceb,
       xlim = c(0, 25),
       col = grey(0.4),
       border = 'white',
       breaks = br,
       xlab = 'children ever born',
       main = title)
}

dev.off()

surveys <- sort(unique(combined_dup$nid))

# define name of plot (children ever died)
pdf('<<<< FILEPATH REDACTED >>>>>/Africa CBH_SBH - SBH ced.pdf',
    width = 8.27,
    height = 11.29)
par(mfrow = c(4, 3))

br <- seq(0, 25, by = 1)
for (s in seq_along(surveys)) {
  sub <- combined_dup[combined_dup$nid == surveys[s], ]
  # make one record per mother
  sub <- unique(sub[c('nid', 'country', 'year', 'mid', 'ced')])
  # remove mothers where number of children born is unknown
  sub <- sub[!is.na(sub$ced), ]
  # set title for the plot
  title <- paste(unique(sub$country), unique(sub$year), unique(sub$nid), sep = " ")

  hist(sub$ced,
       xlim = c(0, 25),
       col = grey(0.4),
       border = 'white',
       breaks = br,
       xlab = 'children ever died',
       main = title)
}

dev.off()

# check that the number of lines per mother matches her reported number of children ever born
# 1) get a frequency count for each unique mother id
mother_ids <- count(combined2[c('nid', 'mid')])
nids_count <- count(mother_ids$nid)
mother_ids$uniq_mid <- paste(mother_ids$nid, mother_ids$mid, sep = "_")

# 2) get the number of children ever born for each mother
mother_ceb <- unique(combined2[c('nid', 'mid', 'ceb')])
mother_ceb$uniq_mid <- paste(mother_ceb$nid, mother_ceb$mid, sep = "_")

# match the two datasets
mother_match_idx <- match(mother_ids$uniq_mid, mother_ceb$uniq_mid)
mother_ids$ceb <- mother_ceb$ceb[mother_match_idx]

# 3) change the entries for 1 row per mother, to 0, to compare against ceb
# be cautious the mothers with 1 ceb, will still be 1 row also
mother_ids$count_corr <- mother_ids$freq
mother_ids$count_corr[mother_ids$count_corr == 1] <- 0

# 4) compare frequency of mother, with number of children ever born
# assumming that there's one record per child, the frequency should match ceb
mother_check <- which(!(mother_ids$ceb == mother_ids$count_corr))

# for those that don't match, check if 1 observation is 1 child born, if so, all is okay!
mother_errors <- mother_ids[mother_check, ]

mother_errors <- mother_errors[mother_errors$ceb > 1, ]

# get list of problematic NIDs; check variables used to generate MID
dodgy_nid <- count(mother_errors$nid)

message('Investigate dodgy NIDs')

# get stats for data in a consolidated dataframe
surveys <- sort(unique(combined$nid))
checks <- data.frame(survey = surveys)

for (s in seq_along(surveys)){
  sub <- combined[combined$nid == surveys[s], ]

  # fill in country and year variables
  checks$country[checks$survey==surveys[s]] <- unique(sub$country)
  checks$year[checks$survey==surveys[s]] <- unique(sub$year)
  checks$survey_id[checks$survey==surveys[s]] <- unique(sub$survey)
  checks$admin_1[checks$survey==surveys[s]] <- length(unique(sub$admin_1))
  #checks$admin_1[checks$survey==surveys[s]] <- ifelse((any(is.na(sub$admin_1)) == TRUE), (length(unique(sub$admin_1)) -1), checks$admin_1)
  checks$admin_2[checks$survey==surveys[s]] <- length(unique(sub$admin_2))
  #checks$admin_2[checks$survey==surveys[s]] <- ifelse((any(is.na(sub$admin_2)) == TRUE), (length(unique(sub$admin_2)) -1), checks$admin_2)

  # get unique for each mother (this will be used for the sbh variables later)
  sbh_1_mother <- unique(sub[c('mid', 'ceb', 'ced', 'mothers_age')])

  # remove records where ceb is.na (these values complicate the below calcs, and should be in
  # the sbh_1_mother above, for reference later)
  sub <- sub[!is.na(sub$ceb), ]

  ### cbh specific stats
  # generate max, min and missing birth to interview for each survey
  checks$min_birth_to_interview[checks$survey==surveys[s]] <- min(sub$birthtointerview_cmc, na.rm = TRUE)
  checks$max_birth_to_interview[checks$survey==surveys[s]] <- max(sub$birthtointerview_cmc, na.rm = TRUE)
  checks$missing_birth_to_interview[checks$survey==surveys[s]] <- sum(is.na(sub$birthtointerview_cmc[sub$ceb != 0]))

  # generate age at death months if child is not alive
  checks$min_aod[checks$survey==surveys[s]] <- min(sub$child_age_at_death_months[sub$child_alive == "No"], na.rm = TRUE)
  checks$max_aod[checks$survey==surveys[s]] <- max(sub$child_age_at_death_months[sub$child_alive == "No"], na.rm = TRUE)
  checks$missing_aod[checks$survey==surveys[s]] <- sum(is.na(sub$child_age_at_death_months[sub$ceb != 0]))

  # generate totals of children alive and dead, and compare cbh totals with reported sbh totals
  checks$children_alive_cbh[checks$survey==surveys[s]] <- sum(!is.na(sub$child_alive[sub$child_alive == "Yes" & sub$ceb != 0]))
  checks$children_alive_sbh[checks$survey==surveys[s]] <- sum(sbh_1_mother$ceb - sbh_1_mother$ced, na.rm = TRUE)

  checks$children_dead_cbh[checks$survey==surveys[s]] <- sum(!is.na(sub$child_alive[sub$child_alive == "No" & sub$ceb != 0]))
  checks$children_dead_sbh[checks$survey==surveys[s]] <- sum(sbh_1_mother$ced, na.rm = TRUE)

  checks$missing_child_survival[checks$survey==surveys[s]] <- sum(is.na(sub$child_alive[sub$ceb != 0]))

  checks$children_dead_match[checks$survey==surveys[s]] <- checks$children_dead_sbh[checks$survey==surveys[s]] == checks$children_dead_cbh[checks$survey==surveys[s]]
  checks$children_alive_match[checks$survey==surveys[s]] <- checks$children_alive_sbh[checks$survey==surveys[s]] == checks$children_alive_cbh[checks$survey==surveys[s]]
  checks$children_dead_per_disc[checks$survey==surveys[s]] <- abs(100-(checks$children_dead_sbh[checks$survey==surveys[s]]/checks$children_dead_cbh[checks$survey==surveys[s]])*100)
  checks$children_alive_per_disc[checks$survey==surveys[s]] <- abs(100-(checks$children_alive_sbh[checks$survey==surveys[s]]/checks$children_alive_cbh[checks$survey==surveys[s]])*100)

  # check if number ever born exceeds number of children in cbh
  checks$children_total_sbh[checks$survey==surveys[s]] <- sum(sbh_1_mother$ceb, na.rm = TRUE)
  checks$children_total_cbh[checks$survey==surveys[s]] <- sum(!is.na(sub$child_alive))

  checks$children_total_match[checks$survey==surveys[s]] <- checks$children_total_sbh[checks$survey==surveys[s]] == checks$children_total_cbh[checks$survey==surveys[s]]

  ### sbh specific stats
  # generate max, min and missing for children ever born
  checks$min_ceb[checks$survey==surveys[s]] <- min(sbh_1_mother$ceb, na.rm = TRUE)
  checks$max_ceb[checks$survey==surveys[s]] <- max(sbh_1_mother$ceb, na.rm = TRUE)
  checks$missing_ceb[checks$survey==surveys[s]] <- sum(is.na(sbh_1_mother$ceb))

  # generate max, min and missing for children ever died
  checks$min_ced[checks$survey==surveys[s]] <- min(sbh_1_mother$ced, na.rm = TRUE)
  checks$max_ced[checks$survey==surveys[s]] <- max(sbh_1_mother$ced, na.rm = TRUE)
  checks$missing_ced[checks$survey==surveys[s]] <- sum(is.na(sbh_1_mother$ced))

  # sum the number of women respondents
  checks$sum_women[checks$survey==surveys[s]] <- nrow(sbh_1_mother)
  checks$missing_mother_ages[checks$survey==surveys[s]] <- sum(is.na(sbh_1_mother$mothers_age))
}

# Inf or -Inf means not in dataset, replace
checks$min_aod[checks$min_aod==Inf] <- NA
checks$max_aod[checks$max_aod==-Inf] <- NA

# write out check dataframe, and investigate columns
write.csv(checks, "<<<< FILEPATH REDACTED >>>>>/Africa post-loc match checks.csv", row.names = FALSE)

# write out the combined dataset;
# create one which is a subset of years
post_98 <- combined[combined$year >= 1998, ]

egy <- post_98[post_98$nid == 19511, ]
ken <- post_98[post_98$nid == 203654, ]

write.csv(combined, "<<<< FILEPATH REDACTED >>>>>/Africa CBH_SBH.csv", row.names = FALSE)

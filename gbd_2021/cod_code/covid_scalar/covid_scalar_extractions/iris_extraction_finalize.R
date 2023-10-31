## EMPTY THE ENVIRONMENT
rm(list = ls())

library(data.table)
library(openxlsx)
library(stringr)

source(paste0("filepath/get_location_metadata.R"))

iris <- read.xlsx(paste0(getwd(),"/iris_extractions.xlsx"))
iris <- data.table(iris)
iris <- iris[,!"week"]

nids <- fread(paste0(getwd(), "/iris_nids.csv"))
nids$Title <- tolower(nids$Title)
split <- str_split_fixed(nids$Title, ": ", 2)
split <- data.table(split)
nids <- cbind(nids, split$V2)
setnames(nids, "V2", "cause_name")
nids[cause_name %like% "haemophilus influenzae", cause_name := "h. influenzae"]
nids[cause_name %like% "streptococcus pneumoniae", cause_name := "s. pneumoniae"]
nids[cause_name %like% "neisseria meningitidis", cause_name := "n. meningitidis"]
nids[cause_name %like% "streptococcus agalactiae", cause_name := "s. agalactiae"]

index <- match(paste0(iris$location_name, iris$cause_name), paste0(nids$Geography, nids$cause_name))
iris$nid <- nids$Nid[index]

# match rows still missing nids
iris[location_name %like% "Czech Republic", location_name:="Czechia"]
iris[location_name %like% "England", location_name:="United Kingdom, England"]
iris[location_name %like% "Hong Kong", location_name:="China, Hong Kong"]
iris[location_name %like% "Northern Ireland", location_name:="United Kingdom, Northern Ireland"]
iris[location_name %like% "Scotland", location_name:="United Kingdom, Scotland"]
iris[location_name %like% "Wales", location_name:="United Kingdom, Wales"]
iris[location_name %like% "South Korea", location_name:="Republic of Korea"]
index <- match(paste0(iris$location_name, iris$cause_name), paste0(nids$Geography, nids$cause_name))
iris$nid <- nids$Nid[index]


# get location ids
locs <- get_location_metadata(location_set_id=35, gbd_round_id=7)
index <- match(iris$location_name, locs$location_name)
iris$location_id <- locs$location_id[index]
iris$ihme_loc_id <- locs$ihme_loc_id[index]
iris[location_name %like% "China, Hong Kong", ihme_loc_id:=locs[location_name %like% "Hong Kong Special Administrative Region of China"]$ihme_loc_id]
iris[location_name %like% "China, Hong Kong", location_id:=locs[location_name %like% "Hong Kong Special Administrative Region of China"]$location_id]

iris[location_name %like% "United Kingdom, England",
     ihme_loc_id:=locs[location_name == "England"]$ihme_loc_id]
iris[location_name %like% "United Kingdom, Northern Ireland",
     ihme_loc_id:=locs[location_name %like% "Northern Ireland"]$ihme_loc_id]
iris[location_name %like% "United Kingdom, Wales",
     ihme_loc_id:=locs[location_name %like% "Wales"]$ihme_loc_id]
iris[location_name %like% "United Kingdom, Scotland",
     ihme_loc_id:=locs[location_name %like% "Scotland"]$ihme_loc_id]

iris[location_name %like% "United Kingdom, England",
     location_id:=locs[location_name == "England"]$location_id]
iris[location_name %like% "United Kingdom, Northern Ireland",
     location_id:=locs[location_name %like% "Northern Ireland"]$location_id]
iris[location_name %like% "United Kingdom, Wales",
     location_id:=locs[location_name %like% "Wales"]$location_id]
iris[location_name %like% "United Kingdom, Scotland",
     location_id:=locs[location_name %like% "Scotland"]$location_id]


# merge with other extractions
recent <- read.xlsx("/filepath/2021_06_07_all_extractions.xlsx")
recent <- rbindlist(list(recent, iris))

write.xlsx(recent, "/filepath/2021_06_10_all_extractions.xlsx")

## Assembles files and completes aggregations from rrMaxCalc output files

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster


## LOAD DEPENDENCIES -----------------------------------------------------
library("data.table")
library("dplyr")
library(argparse)
source("../../helper_functions/csv2objects.R")
library(ggplot2)




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--config_file", help = "path to config file",
                    default = "/FILEPATH/config20210706.csv", type = "character")
parser$add_argument("--outdir", help = "directory to which results will be output",
                    default = "/FILEPATH/config20210706", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


#config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config_file, exclude = ls())


## ASSEMBLE FILES AND COMPLETE AGGREGATIONS ------------------------------
sevs <- do.call(rbind, lapply(causeList, function(cause) {fread(paste0(outdir, "/sevs_", cause, ".csv"))}))

all <- sevs[, lapply(.SD, function(x) {mean(x, na.rm =T)}), by = c("location_id", "year_id", "risk", "rei_id", "age_group_id", "sex_id"), .SDcols = "sev"]
all[, acause := "_all"]

sevs <- rbind(sevs, all)

all[risk=="cold", riskLab := "Low temperature"][risk=="heat", riskLab := "High temperature"]

col.values <- c("red", "navy")

all %>% filter(location_id==1) %>% ggplot(aes(x = year_id, y = sev, color = riskLab)) + 
  geom_point() + geom_line() + theme_minimal() +
  scale_color_manual(values = col.values) +
  labs(color = "") +  xlab("Year") + ylab("SEV") + 
  theme_minimal() + theme(text = element_text(size=16, color = "black"))


ggsave(paste0(outdir, "/sevTrends.png"), width = 14, height = 6)
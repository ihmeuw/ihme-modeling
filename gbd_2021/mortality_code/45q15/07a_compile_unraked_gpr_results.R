# merge stage 1 and stage 2 results, calculate gpr mean, lower, and upper


# ----- Set up environment -----
rm(list=ls())
library(data.table)
library(assertable)
library(argparse)
parser <- ArgumentParser()
parser$add_argument('--ihme_loc_id', type="character", required=TRUE,
                    help='Country to run')
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='Version id of 45q15 model run')

args <- parser$parse_args()
version_id <- args$version_id
ihme_loc_id <- args$ihme_loc_id
working_dir <- paste0("FILEPATH")

#
codes <- fread(paste0("FILEPATH"))
parents <- codes[codes$level_1==1 & codes$level_2==0,]
nonparents <- codes[!(codes$ihme_loc_id %in% parents$ihme_loc_id),]
nonparents <- nonparents[nonparents$level == 3 | nonparents$ihme_loc_id %in% c("CHN_354", "CHN_361"),]

# ----- Import gpr results and merge first and second stage results onto data -----

model_results <- fread(paste0("FILEPATH"))
model_results <- unique(model_results[, list(pred.1.wRE, pred.1.noRE, pred.2.final, sex, year, ihme_loc_id)])

input_draws <- assertable::import_files(filenames = c(paste0("FILEPATH"),
                                                      paste0("FILEPATH")),
                                        folder = paste0("FILEPATH"),
                                        multicore = T, mc.cores = 5,
                                        use.names = T)

input_draws <- merge(input_draws, model_results, by = c("sex", "year", "ihme_loc_id"), all.x = T)

sims_data <- copy(input_draws)

setkey(input_draws,ihme_loc_id,sex,year)
input_draws <- input_draws[,list(mort_med = mean(mort),
                                 mort_lower = quantile(mort,probs = .025),
                                 mort_upper = quantile(mort,probs = .975),
                                 med_stage1 = quantile(pred.1.noRE, .5),
                                 med_stage2 = quantile(pred.2.final, .5)),
                          by = key(input_draws)]

for (selected_sex in unique(sims_data[, sex])) {
  if (ihme_loc_id %in% unique(nonparents$ihme_loc_id)) {
    filename <- paste0("FILEPATH")
    fwrite(sims_data[sex == selected_sex, ], filename)
  } else {
    filename <- paste0("FILEPATH")
    fwrite(sims_data[sex == selected_sex, ], filename)
  }
}

for (selected_sex in unique(input_draws[, sex])) {
  if (ihme_loc_id %in% unique(nonparents$ihme_loc_id)) {
    filename <- paste0("FILEPATH")
    fwrite(input_draws[sex == selected_sex, ], filename)
  } else {
    filename <- paste0("FILEPATH")
    fwrite(input_draws[sex == selected_sex, ], filename)

  }
}

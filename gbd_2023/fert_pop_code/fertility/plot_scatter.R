##############################
## Purpose: Scatter two versions
## Details: Generate scatter plots of old
##          and new versions of TFR
#############################

rm(list=ls())
library(data.table)
library(readr)
library(boot)
library(assertable)
library(RMySQL)
library(ggplot2)
library(ggrepel)
library(mortdb)
library(mortcore)

user <- "USERNAME"
if (interactive()){
  version1 <- "Recent run id"
  version2 <- "Bested run id"
  gbd_year <- 2020
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version1', type = 'integer')
  parser$add_argument('--version2', type = 'integer')
  parser$add_argument('--gbd_year', type = 'integer')
  args <- parser$parse_args()
  version1 <- args$version1
  version2 <- args$version2
  gbd_year <- args$gbd_year
}

hostname="modeling-mortality-db"
myconn <- db_init(hostname = hostname, db_permissions = "read_only")
on.exit(tryCatch(DBI::dbDisconnect(myconn), error=function(e){}, 
                 warning = function(w) {}))
old_version <- DBI::dbGetQuery(myconn, paste0('SELECT gbd_round_id FROM ',
                                              'mortality.process_version ',
                                              'WHERE run_id = ', version2, 
                                              ' AND process_id = 36'))
new_version <- DBI::dbGetQuery(myconn, paste0('SELECT gbd_round_id FROM ',
                                              'mortality.process_version ',
                                              'WHERE run_id =', version1, 
                                              ' AND process_id = 36'))
DBI::dbDisconnect(myconn)

if (nrow(old_version) == 0) {
  old_version <- "GBD round id"
}
if (nrow(new_version) == 0) {
  new_version <- "GBD round id"
}

if(old_version < 7) {
  old_tfr <- fread(paste0('FILEPATH/compiled_summary_gpr.csv'))
} else {
  old_tfr <- fread(paste0('FILEPATH/compiled_gpr_results.csv'))
}

if(new_version < 7) {
  new_tfr <- fread(paste0('FILEPATH/compiled_summary_gpr.csv'))
} else {
  new_tfr <- fread(paste0('FILEPATH/compiled_gpr_results.csv'))
}


setnames(old_tfr, 'mean', 'old')
setnames(new_tfr, 'mean', 'new')
plot_tfr <- merge(new_tfr, old_tfr, by = c('year', 'ihme_loc_id', 'age'))
plot_tfr <- unique(plot_tfr)
plot_tfr <- plot_tfr[age == 'tfr']
plot_tfr[, year_id := floor(year)]
plot_tfr[, year := NULL]
plot_tfr[, pct_diff := abs(old - new) / old * 100]
plot_tfr[pct_diff > 10, group := '>10%']
plot_tfr[pct_diff > 5 & pct_diff <= 10, group := '5-10%']
plot_tfr[pct_diff > 2 & pct_diff <= 5, group := '2-5%']
plot_tfr[pct_diff <= 2, group := '<=2%']

pdf(paste0('FILEPATH/TFR_scatter_', version1, '.pdf'), width=15, height=10)
for(year in c(1950, 1960, 1970, 1980, 1990, 2000, 2010, 2019)) {
  pct99 <- quantile(plot_tfr[year_id == year, pct_diff], c(.99))
  g <- ggplot(plot_tfr[year_id == year], aes(new, old, color = group)) + 
    geom_abline(slope=1, intercept=0) +
    geom_point() + 
    geom_text_repel(data = plot_tfr[year_id == year & pct_diff >= pct99], 
                    aes(x = new, y = old, label = ihme_loc_id), color = 'darkgray') +
    labs(title = paste0('TFR Estimate Comparsion: Year ', year)) +
    ylab(paste0("Version ", version2))
  print(g)
}
dev.off()

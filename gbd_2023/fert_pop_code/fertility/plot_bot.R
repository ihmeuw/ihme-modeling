# Description:
# Alert fertility channel when diagnostic plots are finished

# ------------------------------------------------------------------------------

rm(list=ls())
library(argparse)
library(mortdb)

parser <- argparse::ArgumentParser()
parser$add_argument('--version1', type = 'character')

args <- parser$parse_args()
version1 <- args$version1

graph_dir <- 'FILEPATH'

files_exist <- F

if(file.exists(fs::path('FILEPATH/plots_', version1, '_appended.pdf'))) {
  files_exist <- T
}

if(files_exist){
  mortdb::send_slack_message(message = paste0('Fertility plots finished. Version: ', 
                                              version1, ' \n',
                                              graph_dir, version1, '/plots_', version1, '_appended.pdf'), 
                             channel = '#fertility', 
                             icon = ':chart_with_downwards_trend:', 
                             botname = 'PlotBot')
} else {
  mortdb::send_slack_message(message = paste0('Fertility plots missing :disappointed:'), 
                             channel = '#fertility', 
                             icon = ':chart_with_downwards_trend:', 
                             botname = 'PlotBot')
}

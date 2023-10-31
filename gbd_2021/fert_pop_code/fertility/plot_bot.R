# Description:
# Alert fertility channel when diagnostic plots are finished

# ------------------------------------------------------------------------------

rm(list=ls())
library(argparse)
library(mortdb, lib = paste0("FILEPATH"))

parser <- argparse::ArgumentParser()
parser$add_argument('--version1', type = 'character')

args <- parser$parse_args()
version1 <- args$version1

graph_dir <- 'FILEPATH '

files_exist <- F

if(file.exists("FILEPATH")) {
  files_exist <- T
}

if(files_exist){
  mortdb::send_slack_message(message = paste0('Fertility plots finished. Version: FILEPATH'), 
                             channel = '#fertility', 
                             icon = ':chart_with_downwards_trend:', 
                             botname = 'PlotBot')
} else {
  mortdb::send_slack_message(message = paste0('Fertility plots missing :disappointed:'), 
                             channel = '#fertility', 
                             icon = ':chart_with_downwards_trend:', 
                             botname = 'PlotBot')
}
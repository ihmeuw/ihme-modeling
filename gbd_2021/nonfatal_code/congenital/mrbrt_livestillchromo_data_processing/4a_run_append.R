## ******************************************************************************
##'
##' Purpose: Script to run append_pdf as a cluster job, to append all pdfs
##' Input:   the arguments to append_pdf
## ******************************************************************************

args <- commandArgs(trailingOnly = TRUE)
crosswalked <- as.logical(args[1]
arg_a <- args[2]
arg_b <- args[3]


if (crosswalked == TRUE){
  append_pdf('FILEPATH/', 'crosswalked_bundle_')
} else{
  append_pdf('FILEPATH/', 'agesex_split_bundle_')
}

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- paste0("USERNAME")
}

library('openxlsx')
library('data.table')

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]

print(args)

source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

bundle.v <- save_bundle_version(bundle_id = bun_id, decomp_step = 'step2', include_clinical = TRUE)
bundle.v <- bundle.v$bundle_version_id

bundle.data.v <- get_bundle_version(bundle_version_id = bundle.v, export = TRUE, transform = TRUE)

write.csv(bundle.data.v, file = paste0('FILEPATH'),
          row.names = FALSE)

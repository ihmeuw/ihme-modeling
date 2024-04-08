execR_path <- "FILEPATH"
image_path <- "FILEPATH"

args <- list(format(Sys.time(), "%Y_%m_%d-%H_%M_%S"))

# Submit qsub
mortcore::qsub(glue::glue("harmonizer_pipeline"),
               code = here::here("submit_workflow.R"),
               mem = 4,
               cores = 2,
               wallclock = "24:00:00",
               pass = args,
               shell = execR_path,
               pass_shell = list(i = image_path),
               submit=T)

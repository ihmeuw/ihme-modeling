####################################################
# Nonfirstborn proportion STGPR model results ######
# Diagnostics using plot_gpr #######################
####################################################

source("FILEPATH")
run_id <- 217498
plot_gpr(
  run.id = run_id,
  output.path = paste0(
    "FILEPATH",
    run_id,
    "_plot_gpr.pdf"
  ),
  cluster.project = "proj_nch"
)
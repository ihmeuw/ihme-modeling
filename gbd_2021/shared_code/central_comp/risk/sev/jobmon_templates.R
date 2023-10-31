# Task templates and task parameters for the Jobmon workflow

sev_tool <- jobmonr::tool(name="sev_calculator")
queue <- "all.q"

#-- TASK PARAMETERS ------------------------------------------------------------

# resource needs for rr_max vary by risk. return defaults if no rei passed
get_resources_rr_max <- function(rei = "") {
    hours <- 6
    mem <- "40G"
    if (rei == "metab_bmi_adult") {
        hours <- 48
        mem <- "200G"
    }
    if (rei == "nutrition_iron") mem <- "100G"
    if (rei == "smoking_shs") {
        hours <- 36
        mem <- "400G"
    }
    resources_rr_max <- list(
        "cores"=10,
        "queue"=queue,
        "runtime"=(60 * 60 * hours),
        "memory"=mem
    )
    return(resources_rr_max)
}

resources_agg_rr <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 30),
    "memory"="2G"
)

resources_sev_calc <- list(
    "cores"=2,
    "queue"=queue,
    "runtime"=(60 * 30),
    "memory"="20G"
)

resources_temp_etl <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 5),
    "memory"="2G"
)

resources_loc_agg <- list(
    "cores"=10,
    "queue"=queue,
    "runtime"=(60 * 60 * 24),
    "memory"="45G"
)

resources_summary <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 60 * 24),
    "memory"="12G"
)

resources_summary_rrmax <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 30),
    "memory"="2G"
)

resources_upload <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 60 * 24),
    "memory"="46G"
)

resources_compare_version <- list(
    "cores"=1,
    "queue"=queue,
    "runtime"=(60 * 5),
    "memory"="2G"
)
#-- TASK TEMPLATES -------------------------------------------------------------

# tasks to calculate RR max for most-detailed risks
template_rr_max <- jobmonr::task_template(
    tool=sev_tool,
    template_name="rr_max",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", get_resources_rr_max()$cores,
        " {rshell} {scriptname} {rei_id} {gbd_round_id} {decomp_step} {year_id} ",
        "{n_draws} {out_dir}")),
    node_args=list("rei_id"),
    task_args=list("gbd_round_id", "decomp_step", "year_id", "n_draws", "out_dir"),
    op_args=list("rshell", "scriptname")
)
set_default_template_resources(
    task_template=template_rr_max,
    default_cluster_name=cluster,
    resources=get_resources_rr_max()
)

# tasks to calculate RR max for risk aggregates
template_agg_rr <- jobmonr::task_template(
    tool=sev_tool,
    template_name="agg_rr",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_agg_rr$cores,
        " {rshell} {scriptname} {agg_rei_id} {child_rei_ids} {gbd_round_id} {n_draws} {out_dir}")),
    node_args=list("agg_rei_id"),
    task_args=list("child_rei_ids", "gbd_round_id", "n_draws", "out_dir"),
    op_args=list("rshell", "scriptname")
)
set_default_template_resources(
    task_template=template_agg_rr,
    default_cluster_name=cluster,
    resources=resources_agg_rr
)

# tasks to calculate SEVs for every location/risk
template_sev_calc <- jobmonr::task_template(
    tool=sev_tool,
    template_name="sev_calc",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_sev_calc$cores,
        " {rshell} {scriptname} {rei_id} {location_id} {year_id} {n_draws} {gbd_round_id} ",
        "{decomp_step} {out_dir} {paf_version} {codcorrect_version} {como_version} ",
        "{by_cause}")),
    node_args=list("location_id", "rei_id"),
    task_args=list(
        "year_id", "n_draws", "gbd_round_id", "decomp_step", "out_dir", "paf_version",
        "codcorrect_version", "como_version", "by_cause"),
    op_args=list("rshell", "scriptname")
)
set_default_template_resources(
    task_template=template_sev_calc,
    default_cluster_name=cluster,
    resources=resources_sev_calc
)

# tasks to pull in externally-generated SEVs for temperature risks
template_temp_etl <- jobmonr::task_template(
    tool=sev_tool,
    template_name="temp_etl",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_temp_etl$cores,
        " {python} {scriptname} --location_id {location_id} --year_id {year_id} ",
        "--n_draws {n_draws} --sev_version_id {sev_version_id} ",
        "--gbd_round_id {gbd_round_id} --decomp_step {decomp_step} --by_cause {by_cause}")),
    node_args=list("location_id"),
    task_args=list("year_id", "n_draws", "sev_version_id", "gbd_round_id", "decomp_step",
        "by_cause"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_temp_etl,
    default_cluster_name=cluster,
    resources=resources_temp_etl
)

# tasks for location aggregation
template_loc_agg <- jobmonr::task_template(
    tool=sev_tool,
    template_name="loc_agg",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_loc_agg$cores,
        " {python} {scriptname} --sev_version_id {sev_version_id} --rei_id {rei_id} ",
        "--location_set_id {location_set_id} --n_draws {n_draws} ",
        "--gbd_round_id {gbd_round_id} --decomp_step {decomp_step} --by_cause {by_cause}")),
    node_args=list("rei_id"),
    task_args=list("sev_version_id", "location_set_id", "n_draws", "gbd_round_id",
        "decomp_step", "by_cause"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_loc_agg,
    default_cluster_name=cluster,
    resources=resources_loc_agg
)

# tasks for creating summaries
template_summary <- jobmonr::task_template(
    tool=sev_tool,
    template_name="summary",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_summary$cores,
        " {python} {scriptname} --sev_version_id {sev_version_id} ",
        "--location_id {location_id} --year_id {year_id} ",
        "--gbd_round_id {gbd_round_id} {change} --by_cause {by_cause}")),
    node_args=list("location_id"),
    task_args=list("sev_version_id", "year_id", "gbd_round_id", "change", "by_cause"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_summary,
    default_cluster_name=cluster,
    resources=resources_summary
)

# tasks for RRmax summaries
template_summary_rrmax <- jobmonr::task_template(
    tool=sev_tool,
    template_name="sum_rrmax",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_summary_rrmax$cores,
        " {python} {scriptname} --sev_version_id {sev_version_id} ",
        "--rei_id {rei_id} --gbd_round_id {gbd_round_id}")),
    node_args=list("rei_id"),
    task_args=list("sev_version_id", "gbd_round_id"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_summary_rrmax,
    default_cluster_name=cluster,
    resources=resources_summary_rrmax
)

# task for uploading results
template_upload <- jobmonr::task_template(
    tool=sev_tool,
    template_name="upload",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_upload$cores,
        " {python} {scriptname} --version_id {version_id} --gbd_round_id {gbd_round_id}",
        " --decomp_step {decomp_step} --pct_change {change} --measure {measure}")),
    node_args=list("measure"),
    task_args=list("version_id", "gbd_round_id", "decomp_step", "change"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_upload,
    default_cluster_name=cluster,
    resources=resources_upload
)

template_compare_version <- jobmonr::task_template(
    tool=sev_tool,
    template_name="compare_version",
    command_template=(paste0(
        "PYTHONPATH= OMP_NUM_THREADS=", resources_compare_version$cores,
        " {python} {scriptname} --version_id {version_id} --gbd_round_id {gbd_round_id}",
        " --decomp_step {decomp_step} --sevs_were_generated {sevs_were_generated}")),
    node_args=list(),
    task_args=list("version_id", "gbd_round_id", "decomp_step", "sevs_were_generated"),
    op_args=list("python", "scriptname")
)
set_default_template_resources(
    task_template=template_compare_version,
    default_cluster_name=cluster,
    resources=resources_compare_version
)

def _argnames_factory(
    task_args: list[str] | None = None, node_args: list[str] | None = None
) -> dict:
    task_args = task_args or []
    node_args = node_args or []
    if "directory" not in task_args:
        task_args = ["directory"] + task_args
    return dict(task_args=task_args, node_args=node_args)


ACTION_ARGNAMES = dict(
    preprocess=_argnames_factory(),
    fit_global=_argnames_factory(node_args=["sex_id"]),
    fit_national=_argnames_factory(node_args=["sex_id", "national_id"]),
    fit_location=_argnames_factory(node_args=["sex_id", "location_id"]),
    fit_kreg=_argnames_factory(node_args=["sex_id", "location_id"]),
    splice_patterns=_argnames_factory(),
    combine_data=_argnames_factory(node_args=["dirpath"]),
    calibrate_uncertainty=_argnames_factory(),
    compute_life_expectancy=_argnames_factory(),
    prepare_handoff=_argnames_factory(),
    create_figures=_argnames_factory(
        node_args=["sex_id", "location_id", "x", "scaled"]
    ),
    create_cmp_figures=_argnames_factory(
        node_args=["sex_id", "location_id", "x", "scaled"]
    ),
    create_coef_figures=_argnames_factory(node_args=["sex_id", "location_id"]),
    create_intercept_figures=_argnames_factory(),
    create_life_expectancy_figures=_argnames_factory(
        node_args=["sex_id", "location_id", "x"]
    ),
    create_cov_figures=_argnames_factory(
        node_args=["sex_id", "location_id", "x"]
    ),
    create_under5_deaths_figures=_argnames_factory(
        node_args=["sex_id", "location_id"]
    ),
    combine_figures=_argnames_factory(),
    create_draws=_argnames_factory(node_args=["sex_id", "location_id"]),
    cleanup_dir=_argnames_factory(node_args=["dirpath"]),
)

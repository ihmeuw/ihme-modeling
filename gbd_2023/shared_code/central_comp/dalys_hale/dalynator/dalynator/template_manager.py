from typing import Any, Dict, List

from gbd.enums import Cluster
from jobmon.client.tool import Tool
from jobmon.client.task_template import TaskTemplate

class TemplateManager:
    """ The TemplateManager handles creation of Jobmon task templates
    and executor parametors for Burdenator and DALYnator jobs. """

    COMPUTATION_QUEUE = "all.q"
    UPLOAD_QUEUE = "all.q"
    TOOL_NAME = None
    CLUSTER = Cluster.SLURM.value
    CORES = "cores"
    MEMORY = "memory"
    RUNTIME = "runtime"
    QUEUE = "queue"


    def __init__(self, jobmon_tool: Tool):
        self.jobmon_tool = jobmon_tool


    @staticmethod
    def get_template_manager(jobmon_tool: Tool, tool_name: str):
        if tool_name == "burdenator":
            return BurdenatorTemplateManager(
                jobmon_tool=jobmon_tool,
            )
        elif tool_name == "dalynator":
            return DalynatorTemplateManager(
                jobmon_tool=jobmon_tool,
            )


    def get_loc_agg_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="location_aggregation",
            command_template=(
                "{script} "
                "--location_set_id {location_set_id} "
                "--years {years} "
                "--sex_id {sex_id} "
                "--sequential_num {sequential_num} "
                "--measure_id {measure_id} "
                "--rei_id {rei_id} "
                "--data_root {data_root} "
                "--release_id {release_id} "
                "--output_version {output_version} "
                "--n_draws {n_draws} "
                "--region_locs {region_locs} "
                "{verbose_flag} "
                "{star_ids_flag} "
            ),
            node_args=[
                "location_set_id",
                "years",
                "sex_id",
                "measure_id",
                "rei_id",
                "sequential_num"
            ],
            task_args=[
                "data_root",
                "release_id",
                "output_version",
                "n_draws",
                "region_locs",
                "verbose_flag",
                "star_ids_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
        )
        return template


    def get_cleanup_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="cleanup",
            command_template=(
                "{script} "
                "--measure_id {measure_id} "
                "--location_id {location_id} "
                "--years {years} "
                "--input_data_root {input_data_root} "
                "--out_dir {out_dir} "
                "--n_draws {n_draws} "
                "--{cod_type} {cod_version} "
                "--epi {epi} "
                "--output_version {output_version} "
                "--release_id {release_id} "
                "--age_group_ids {age_group_ids} "
                "--age_group_set_id {age_group_set_id} "
                "{verbose_flag} "
                "{null_nan_check_flag} "
                "{star_ids_flag} "
            ),
            node_args=["measure_id", "location_id", "years"],
            task_args=[
                "input_data_root",
                "out_dir",
                "n_draws",
                "cod_type",
                "cod_version",
                "epi",
                "output_version",
                "release_id",
                "age_group_ids",
                "age_group_set_id",
                "verbose_flag",
                "null_nan_check_flag",
                "star_ids_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
        )
        return template


    def get_public_sort_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="public_sort",
            command_template=(
                "{script} "
                "--gbd_process_id {gbd_process_id} "
                "--table_type {table_type} "
                "--measure_id {measure_id} "
                "--location_id {location_id} "
                "--out_dir {out_dir} "
                "{upload_to_test_flag} "
            ),
            node_args=[
                "gbd_process_id",
                "table_type",
                "measure_id",
                "location_id",
            ],
            task_args=[
                "out_dir",
                "upload_to_test_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 4,
                self.MEMORY: "8G",
                self.RUNTIME: 60 * 60 * 4,
                self.QUEUE: self.COMPUTATION_QUEUE,
            },
        )
        return template


    def get_pct_change_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="pct_change",
            command_template=(
                "{script} "
                "--measure_id {measure_id} "
                "--start_year {start_year} "
                "--end_year {end_year} "
                "--location_id {location_id} "
                "--input_data_root {input_data_root} "
                "--out_dir {out_dir} "
                "--tool_name {tool_name} "
                "--release_id {release_id} "
                "--age_group_ids {age_group_ids} "
                "--output_version {output_version} "
                "--n_draws {n_draws} "
                "--{cod_type} {cod_version} "
                "--epi {epi} "
                "{verbose_flag} "
                "{star_ids_flag} "
            ),
            node_args=["measure_id", "start_year", "end_year", "location_id"],
            task_args=[
                "input_data_root",
                "out_dir",
                "tool_name",
                "release_id",
                "age_group_ids",
                "output_version",
                "n_draws",
                "cod_type",
                "cod_version",
                "epi",
                "verbose_flag",
                "star_ids_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 1,
                self.MEMORY: "140G",
                self.RUNTIME: 60 * 60 * 2,
                self.QUEUE: self.COMPUTATION_QUEUE,
            },
        )
        return template


    def get_public_sync_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="public_sync",
            command_template=(
                "{script} "
                "--gbd_process_version_ids {gbd_process_version_ids} "
                "{upload_to_test_flag} "
            ),
            task_args=["gbd_process_version_ids", "upload_to_test_flag"],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 1,
                self.MEMORY: "2G",
                self.RUNTIME: 60 * 60,
                self.QUEUE: self.COMPUTATION_QUEUE,
            },
        )
        return template


    def get_upload_template(
            self, table_type, measure_id, gbd_process_version_id
        ) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name=f"upload_{table_type}_{measure_id}_v{gbd_process_version_id}",
            command_template=(
                "{script} "
                "--gbd_process_version_id {gbd_process_version_id} "
                "--table_type {table_type} "
                "--out_dir {out_dir} "
                "--location_id {location_id} "
                "--measure_id {measure_id} "
                "--year_ids {year_ids} "
                "{upload_to_test_flag} "
            ),
            node_args=[
                "gbd_process_version_id",
                "table_type",
                "measure_id",
                "location_id"
            ],
            task_args=[
                "out_dir",
                "year_ids",
                "upload_to_test_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
            self.CORES: 1,
            self.MEMORY: "6G",
            self.RUNTIME: 60 * 60 * 24,
            self.QUEUE: self.UPLOAD_QUEUE,
            },
        )
        return template


    def get_public_upload_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name="public_upload",
            command_template=(
                "{script} "
                "--gbd_process_id {gbd_process_id} "
                "--table_type {table_type} "
                "--measure_id {measure_id} "
                "--location_id {location_id} "
                "--gbd_process_version_id {gbd_process_version_id} "
                "--out_dir {out_dir} "
                "{upload_to_test_flag} "
            ),
            node_args=[
                "gbd_process_id",
                "table_type",
                "measure_id",
                "location_id",
            ],
            task_args=[
                "gbd_process_version_id",
                "out_dir",
                "upload_to_test_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 1,
                self.MEMORY: "2G",
                self.RUNTIME: 60 * 30,
                self.QUEUE: self.COMPUTATION_QUEUE,
            },
        )
        return template


    def get_loc_agg_resources(self, year_ids: List[int]) -> Dict[str, Any]:
        """Location aggregation jobs have varying resource needs based on the
        number of year IDs passed into the run.
        """
        return {
            self.CORES: 10,
            self.MEMORY: f"{2*len(year_ids)}G",
            self.RUNTIME: f"{1000*len(year_ids)}s",
            self.QUEUE: self.COMPUTATION_QUEUE,
        }


    def get_cleanup_resources(self, year_ids: List[int]) -> Dict[str, Any]:
        """Cleanup jobs have varying resource needs based on the number of year
        IDs passed into the run.
        """
        return {
            self.CORES: 1,
            self.MEMORY: f"{4*len(year_ids)}G",
            self.RUNTIME: f"{500*len(year_ids)}s",
            self.QUEUE: self.COMPUTATION_QUEUE,
        }


class BurdenatorTemplateManager(TemplateManager):

    TOOL_NAME = "burdenator"


    def get_most_detailed_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name=f"{BurdenatorTemplateManager.TOOL_NAME}_most_detailed",
            command_template=(
                "{script} "
                "--location_id {location_id} "
                "--year_id {year_id} "
                "--input_data_root {input_data_root} "
                "--out_dir {out_dir} "
                "--tool_name {tool_name} "
                "--{cod_type} {cod_version} "
                "--epi {epi} "
                "--paf_version {paf_version} "
                "--n_draws {n_draws} "
                "--output_version {output_version} "
                "--release_id {release_id} "
                "--age_group_ids {age_group_ids} "
                "--age_group_set_id {age_group_set_id} "
                "--measure_ids {measure_ids} "
                "{verbose_flag} "
                "{paf_error_flag} "
                "{skip_cause_agg_flag} "
                "{null_nan_check_flag} "
                "{star_ids_flag} "
            ),
            node_args=["location_id", "year_id"],
            task_args=[
                "input_data_root",
                "out_dir",
                "tool_name",
                "cod_type",
                "cod_version",
                "epi",
                "paf_version",
                "n_draws",
                "output_version",
                "release_id",
                "age_group_ids",
                "age_group_set_id",
                "measure_ids",
                "verbose_flag",
                "paf_error_flag",
                "skip_cause_agg_flag",
                "null_nan_check_flag",
                "star_ids_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 6,
                self.MEMORY: "75G",
                self.RUNTIME: 60 * 60 * 3,
                self.QUEUE: BurdenatorTemplateManager.COMPUTATION_QUEUE,
            },
        )
        return template


class DalynatorTemplateManager(TemplateManager):

    TOOL_NAME = "dalynator"


    def get_most_detailed_template(self) -> TaskTemplate:
        template = self.jobmon_tool.get_task_template(
            template_name=f"{DalynatorTemplateManager.TOOL_NAME}_most_detailed",
            command_template=(
                "{script} "
                "--location_id {location_id} "
                "--year_id {year_id} "
                "--input_data_root {input_data_root} "
                "--out_dir {out_dir} "
                "--tool_name {tool_name} "
                "--{cod_type} {cod_version} "
                "--epi {epi} "
                "--n_draws {n_draws} "
                "--output_version {output_version} "
                "--release_id {release_id} "
                "--age_group_ids {age_group_ids} "
                "--age_group_set_id {age_group_set_id} "
                "{verbose_flag} "
                "{null_nan_check_flag} "
            ),
            node_args=["location_id", "year_id"],
            task_args=[
                "input_data_root",
                "out_dir",
                "tool_name",
                "cod_type",
                "cod_version",
                "epi",
                "n_draws",
                "output_version",
                "release_id",
                "age_group_ids",
                "age_group_set_id",
                "verbose_flag",
                "null_nan_check_flag",
            ],
            op_args=["script"],
            default_cluster_name=self.CLUSTER,
            default_compute_resources={
                self.CORES: 6,
                self.MEMORY: "20G",
                self.RUNTIME: 60 * 60 * 2,
                self.QUEUE: DalynatorTemplateManager.COMPUTATION_QUEUE,
            },
        )
        return template

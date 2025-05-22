import sys
from loguru import logger
from pathlib import Path
from typing import List, Dict
import datetime
import uuid
import shutil
import pickle

import warnings
warnings.simplefilter('ignore')

from jobmon.client.tool import Tool
from covid_gbd_model.variables import RELEASE_ID
from covid_gbd_model.paths import (
    OUTPUT_ROOT,
    make_version_path,
)

DROP_COVS = ['total_covid_asdr', 'uhc', 'cum_inf', 'cum_eff_vacc']
COMBINE_ALPHA_BETA_GAMMA_DELTA = True
COMBINE_OMICRON_BA5 = True

SPLICING_SCHEMA = {
    # regions with some VR data (use super region if all regions fall here) -->
    'g_sr_r_l': [
        31,   # Central Europe, Eastern Europe, and Central Asia (super region)
        64,   # High-income (super region)
        103,  # Latin America and Caribbean (super region)
        137,  # North Africa and Middle East (super region)
        21,   # Oceania (region)
        9,    # Southeast Asia (region)
    ],
    # regions with no VR data (use super region if all regions fall here) -->
    'g_l': [
        158,  # South Asia (super region)
        166,  # Sub-Saharan Africa (super region)
        5,    # East Asia (region)
    ],
}
PROJECT_2023 = True
CHINA_2024_SCALAR = 0.2
## distribution of 2023/2022 across all locations
# min        0.003861
# 25%        0.125451
# 50%        0.200722
# 75%        0.270652
# max       21.986591


def execute_workflow(
    inputs_root: Path,
    age_pattern_model_root: Path,
    idr_adj_model_roots: List[Path],
    spliced_model_root: Path,
):
    wf_uuid = uuid.uuid4()

    tool = Tool(name='covid_mortality_model')

    workflow = tool.create_workflow(
        name=f'covid_mortality_models_{wf_uuid}',
    )

    inputs_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 1,
            'memory': '6G',
            'runtime': '1h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': '',
            'constraints': 'archive',
        },
        template_name='inputs',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("onemod", "covid-gbd-model")}'
                         f' {str(Path(__file__).parent / "data/store_inputs.py")}'
                         ' {inputs_root}',
        node_args=[],
        task_args=['inputs_root'],
        op_args=[],
    )
    model_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 1,
            'memory': '6G',
            'runtime': '3h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': '',
        },
        template_name='model',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("covid-gbd-model", "onemod")}'
                         f' {str(Path(__file__).parent / "model/model.py")}'
                         ' {kwargs_path}',
        node_args=['kwargs_path'],
        task_args=[],
        op_args=[],
    )
    age_sex_splitting_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 1,
            'memory': '2G',
            'runtime': '1h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': '',
        },
        template_name='age_sex_splitting',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("onemod", "covid-gbd-model")}'
                         f' {str(Path(__file__).parent / "model/age_sex_split.py")}'
                         ' {inputs_root} {model_root}',
        node_args=[],
        task_args=['inputs_root', 'model_root'],
        op_args=[],
    )
    postprocessing_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 21,
            'memory': '30G',
            'runtime': '3h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': 'proj_rapidresponse',
        },
        template_name='postprocessing',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("covid-gbd-model", "onemod")}'
                         f' {str(Path(__file__).parent / "postprocessing/postprocessing.py")}'
                         ' {inputs_root} {model_root}',
        node_args=['inputs_root', 'model_root'],
        task_args=[],
        op_args=[],
    )
    diagnostics_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 2,
            'memory': '6G',
            'runtime': '1h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': '',
            'constraints': 'archive',
        },
        template_name='diagnostics',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("covid-gbd-model", "onemod")}'
                         f' {str(Path(__file__).parent / "diagnostics/diagnostics.py")}'
                         ' {inputs_root} {model_root}',
        node_args=['inputs_root', 'model_root'],
        task_args=[],
        op_args=[],
    )
    splicing_template = tool.get_task_template(
        default_compute_resources={
            'queue': 'all.q',
            'cores': 21,
            'memory': '30G',
            'runtime': '1h',
            # 'stdout': str(version_root / '_diagnostics' / 'logs' / 'output'),
            # 'stderr': str(version_root / '_diagnostics' / 'logs' / 'error'),
            'project': '',
            'constraints': 'archive',
        },
        template_name='splicing',
        default_cluster_name='slurm',
        command_template=f'{shutil.which("python").replace("covid-gbd-model", "onemod")}'
                         f' {str(Path(__file__).parent / "postprocessing/splicing.py")}'
                         ' {kwargs_path}',
        node_args=[],
        task_args=['kwargs_path'],
        op_args=[],
    )

    inputs_tasks = [
        inputs_template.create_task(
            max_attempts=1,
            inputs_root=str(inputs_root),
        )
    ]
    age_pattern_model_tasks = [
        model_template.create_task(
            upstream_tasks=inputs_tasks,
            max_attempts=1,
            kwargs_path=str(age_pattern_model_root / 'kwargs.pkl')
        )
    ]
    age_sex_splitting_tasks = [
        age_sex_splitting_template.create_task(
            upstream_tasks=age_pattern_model_tasks,
            max_attempts=1,
            inputs_root=str(inputs_root),
            model_root=str(age_pattern_model_root),
        )
    ]
    idr_adj_model_tasks = []
    idr_adj_postprocessing_tasks = []
    idr_adj_diagnostics_tasks = []
    for idr_adj_model_root in idr_adj_model_roots:
        idr_adj_model_task = model_template.create_task(
            upstream_tasks=age_sex_splitting_tasks,
            max_attempts=1,
            kwargs_path=str(idr_adj_model_root / 'kwargs.pkl'),
        )
        idr_adj_model_tasks.append(idr_adj_model_task)
        idr_adj_postprocessing_task = postprocessing_template.create_task(
            upstream_tasks=[idr_adj_model_task],
            max_attempts=1,
            inputs_root=str(inputs_root),
            model_root=str(idr_adj_model_root),
        )
        idr_adj_postprocessing_tasks.append(idr_adj_postprocessing_task)
        idr_adj_diagnostics_task = diagnostics_template.create_task(
            upstream_tasks=[idr_adj_postprocessing_task],
            max_attempts=1,
            inputs_root=str(inputs_root),
            model_root=str(idr_adj_model_root),
        )
        idr_adj_diagnostics_tasks.append(idr_adj_diagnostics_task)
    splicing_tasks = [
        splicing_template.create_task(
            upstream_tasks=idr_adj_postprocessing_tasks,
            max_attempts=1,
            kwargs_path=str(spliced_model_root / 'kwargs.pkl')
        )
    ]

    workflow.add_tasks(inputs_tasks)
    workflow.add_tasks(age_pattern_model_tasks)
    workflow.add_tasks(age_sex_splitting_tasks)
    workflow.add_tasks(idr_adj_model_tasks)
    workflow.add_tasks(idr_adj_postprocessing_tasks)
    workflow.add_tasks(idr_adj_diagnostics_tasks)
    workflow.add_tasks(splicing_tasks)
    workflow.bind()

    logger.info(f'Running workflow with ID {workflow.workflow_id}.')
    logger.info('For full information see the Jobmon GUI:')
    logger.info(f'https://jobmon-gui.ihme.washington.edu/#/workflow/{workflow.workflow_id}/tasks')

    status = workflow.run(fail_fast=False)
    logger.info(f'Workflow {workflow.workflow_id} completed with status {status}.')


def make_gbd_estimates(
    obs_measure: str,
    n_draws: str,
    as_l: str,
    t_threshold: float,
    description: str,
    config_dir: Path,
    drop_covs: List[str],
    combine_alpha_beta_gamma_delta: bool,
    combine_omicron_ba5: bool,
    splicing_schema: Dict,
    release_id: int,
    project_2023: bool,
    china_2024_scalar: float,
):
    program_start = datetime.datetime.now()

    if release_id != 16 and project_2023:
        raise ValueError('Re-check assumption of projecting 2023 if not GBD 2023.')

    version_root = make_version_path(OUTPUT_ROOT / 'modeling')
    logger.info(f'Running pipeline in {version_root}')

    ## STAGE INPUTS
    inputs_root = version_root / 'inputs'
    inputs_root.mkdir()

    ## AGE PATTERN MODEL
    age_pattern_model_root = version_root / 'age_pattern_model'
    age_pattern_model_root.mkdir()
    age_pattern_model_kwargs = dict(
        preprocessing=dict(
            inputs_root=inputs_root,
            model_root=age_pattern_model_root,
            obs_measure=obs_measure,
            pipeline_stage='age_pattern',
            location_effects=as_l,
            t_threshold=t_threshold,
            n_draws=n_draws,
            description=description,
            config_dir=config_dir,
            drop_covs=drop_covs,
            combine_alpha_beta_gamma_delta=combine_alpha_beta_gamma_delta,
            combine_omicron_ba5=combine_omicron_ba5,
        ),
        model=dict(
            inputs_root=inputs_root,
            model_root=age_pattern_model_root,
            onemod_stages=['fit_spxmod', 'fit_kreg'],
            adjust_idr=False,
        )
    )
    with open(age_pattern_model_root / 'kwargs.pkl', 'wb') as file:
        pickle.dump(age_pattern_model_kwargs, file)

    ## IDR-ADJUSTMENT MODELS
    idr_adj_model_roots = []
    for location_effects in splicing_schema.keys():
        idr_adj_model_root = version_root / f'{location_effects}_model'
        idr_adj_model_root.mkdir()
        idr_adj_model_roots.append(idr_adj_model_root)
        idr_adj_model_kwargs = dict(
            preprocessing=dict(
                inputs_root=inputs_root,
                model_root=idr_adj_model_root,
                obs_measure=obs_measure,
                pipeline_stage='idr_adj',
                location_effects=location_effects,
                t_threshold=t_threshold,
                n_draws=n_draws,
                description=description,
                config_dir=config_dir,
                drop_covs=drop_covs,
                combine_alpha_beta_gamma_delta=combine_alpha_beta_gamma_delta,
                combine_omicron_ba5=combine_omicron_ba5,
            ),
            model=dict(
                inputs_root=inputs_root,
                model_root=idr_adj_model_root,
                onemod_stages=['fit_spxmod', 'fit_kreg', 'draws'],
                adjust_idr=True,
            )
        )
        with open(idr_adj_model_root / 'kwargs.pkl', 'wb') as file:
            pickle.dump(idr_adj_model_kwargs, file)

    spliced_model_root = version_root / 'spliced_model'
    spliced_model_root.mkdir()
    spliced_model_kwargs = dict(
        version_root=version_root,
        splicing_schema=splicing_schema,
        project_2023=project_2023,
        china_2024_scalar=china_2024_scalar,
    )
    with open(spliced_model_root / 'kwargs.pkl', 'wb') as file:
       pickle.dump(spliced_model_kwargs, file)

    execute_workflow(
        inputs_root=inputs_root,
        age_pattern_model_root=age_pattern_model_root,
        idr_adj_model_roots=idr_adj_model_roots,
        spliced_model_root=spliced_model_root,
    )

    ## JOBS FOR GBD PROCESSING AND UPLOAD?

    program_end = datetime.datetime.now()
    runtime = program_end - program_start
    logger.info(f'Model complete -- runtime: {int(runtime.seconds / 60)} minutes')
    logger.info(f'Output found in {version_root}')


if __name__ == '__main__':
    make_gbd_estimates(
        obs_measure=sys.argv[1],
        n_draws=int(sys.argv[2]),
        as_l='g_l',  # default
        t_threshold=0.5,  # default
        description=sys.argv[3],
        config_dir=Path(__file__).parents[0] / 'config',
        drop_covs=DROP_COVS,
        combine_alpha_beta_gamma_delta=COMBINE_ALPHA_BETA_GAMMA_DELTA,
        combine_omicron_ba5=COMBINE_OMICRON_BA5,
        splicing_schema=SPLICING_SCHEMA,
        release_id=RELEASE_ID,
        project_2023=PROJECT_2023,
        china_2024_scalar=CHINA_2024_SCALAR,
    )

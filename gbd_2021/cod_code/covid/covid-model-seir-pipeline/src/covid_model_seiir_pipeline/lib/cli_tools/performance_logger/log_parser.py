from collections import defaultdict
from pathlib import Path

import click
import pandas as pd


@click.command()
@click.argument('log_dir',
                type=click.Path(exists=True, file_okay=False, resolve_path=True))
@click.argument('job_name', type=click.STRING)
def parse_logs(log_dir, job_name):
    result = defaultdict(list)
    log_paths = list(Path(log_dir).glob(f'{job_name}*'))
    if not log_paths:
        click.echo(f'No logs found for job name {job_name}. Exiting.')
        return

    for log_path in log_paths:
        with log_path.open() as f:
            log_data = f.readlines()
        for line in log_data[log_data.index('Runtime report\n') + 2:]:
            metric, time = line.split(':')
            time, *_ = time.split('\x1b')
            result[metric.strip()].append(float(time.strip()))
    df = pd.DataFrame(result)
    summary = df.describe().T
    total = df.sum()
    summary['total'] = total
    click.echo(summary[['mean', 'min', 'max', 'total']])

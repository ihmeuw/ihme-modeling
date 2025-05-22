import fire
from jobmon.client.status_commands import resume_workflow_from_id


def main(
    workflow_id: int, cluster_name: str = "slurm", log: bool = True
) -> None:
    resume_workflow_from_id(
        workflow_id=workflow_id, cluster_name=cluster_name, log=log
    )


if __name__ == "__main__":
    fire.Fire(main)

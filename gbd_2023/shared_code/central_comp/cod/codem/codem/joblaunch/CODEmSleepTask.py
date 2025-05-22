import logging
import uuid
from typing import List, Optional

from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from codem.joblaunch.run_utils import get_jobmon_tool


class CODEmSleepTask:
    """Used for throttling CODEm models that run at the same time."""

    def __init__(self, sleep_time: int, upstream_tasks: Optional[List[Task]] = None):
        self.sleep_time = sleep_time
        self.upstream_tasks = upstream_tasks
        self.name = f"codem_sleep_{self.sleep_time}"

        self.tool = get_jobmon_tool()
        self.command = self.generate_command()
        self.task_template = self.get_task_template()
        # Add unique identifier force Jobmon to create different hashes for sleep tasks that
        # are a part of the same workflow
        self.unique_id = str(uuid.uuid4())

    def generate_command(self) -> str:
        """Generates command for sleeping a certain number of seconds."""
        command = "sleep {sleep_time}; echo {unique_id}"
        return command

    def get_task_template(self) -> TaskTemplate:
        """Gets a task template for running codem_diagnostics for two CODEm models"""
        template_transform = self.tool.get_task_template(
            template_name="codem_sleep",
            command_template=self.command,
            node_args=["sleep_time", "unique_id"],
        )

        return template_transform

    def get_task(self) -> Task:
        """Returns a Jobmon task using the codem_sleep task template."""
        logging.info(f"Sleep task set up, sleeping {self.sleep_time} seconds")
        node_args = {"sleep_time": self.sleep_time, "unique_id": self.unique_id}
        task = self.task_template.create_task(
            max_attempts=1,
            upstream_tasks=self.upstream_tasks,
            name=self.name,
            compute_resources={
                "cores": 1,
                "memory": "256M",
                "runtime": self.sleep_time + 300,  # 5 extra mins
                "queue": "all.q",
            },
            **node_args,
        )
        return task

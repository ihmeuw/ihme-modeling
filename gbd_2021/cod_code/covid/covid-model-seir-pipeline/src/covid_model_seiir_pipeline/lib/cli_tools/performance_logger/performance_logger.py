from collections import defaultdict
import time

from loguru import logger


class TaskPerformanceLogger:

    def __init__(self):
        self.current_context = None
        self.current_context_start = None
        self.times = defaultdict(float)

    def _record_timing(self, context):
        if self.current_context is None:
            self.current_context = context
            self.current_context_start = time.time()
        else:
            self.times[self.current_context] += time.time() - self.current_context_start
            self.current_context = context
            self.current_context_start = time.time()

    def info(self, *args, context=None, **kwargs):
        if context is not None:
            self._record_timing(context)
        logger.info(*args, **kwargs)

    def debug(self, *args, context=None, **kwargs):
        if context is not None:
            self._record_timing(context)
        logger.debug(*args, **kwargs)

    def warning(self, *args, context=None, **kwargs):
        if context is not None:
            self._record_timing(context)
        logger.debug(*args, **kwargs)

    def exception(self, *args, context=None, **kwargs):
        if context is not None:
            self._record_timing(context)
        logger.exception(*args, **kwargs)

    def report(self):
        if self.current_context is not None:
            self.times[self.current_context] += time.time() - self.current_context_start
        times = self.times.copy()
        times['total'] = sum(self.times.values())
        logger.info(
            "\nRuntime report\n" +
            "=" * 31 + "\n" +
            "\n".join([f'{context:<20}:{elapsed_time:>10.2f}' for context, elapsed_time in times.items()])
        )


task_performance_logger = TaskPerformanceLogger()

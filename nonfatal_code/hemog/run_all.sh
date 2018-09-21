#!/bin/bash
source {FILEPATH}
cd {FILEPATH}
python -m tasks Hook --identity process_hook --local-scheduler --workers 4 &> {LOG FILE}

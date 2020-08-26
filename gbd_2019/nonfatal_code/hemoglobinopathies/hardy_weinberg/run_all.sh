#!/bin/bash
source /ihme/code/central_comp/anaconda/bin/activate tasker
cd /ihme/centralcomp/custom_models/hardy_weinberg/hardy_weinberg
python -m tasks Hook --identity process_hook --local-scheduler --workers 4 &> ~/temp/hardy_5.txt

#!/bin/bash
export PATH=$1:$PATH
source activate $2
shift 2
"$@"

#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q $HOME/FILEPATH "$1" "$2" "$3" "$4" "$5"

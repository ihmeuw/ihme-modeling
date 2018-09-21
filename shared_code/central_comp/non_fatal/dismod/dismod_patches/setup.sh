#! /bin/bash -e
# $Id$
#  --------------------------------------------------------------------------
# dismod_ode: MCMC Estimation of Disease Rates as Functions of Age
#           Copyright (C) 2013 University of Washington
#              (Bradley M. Bell USER@uw.edu)
#
# This program is distributed under the terms of the
#	     GNU Affero General Public License version 3.0 or later
# see http://www.gnu.org/licenses/agpl.txt
# -------------------------------------------------------------------------- */
# bash function that echos and executes a command
echo_eval() {
	echo $*
	eval $*
}
# ------------------------------------------------------------------------
if [ "$0" != "bin/setup.sh" ]
then
	echo "bin/setup.sh: must be executed from its parent directory"
	exit 1
fi
# ------------------------------------------------------------------------
# define parameters here
gsl_prefix='/usr'
ihme_data_drive='strDir'
dismod_ode_prefix="/usr"
python_program='strDir'
cxx_flags='-O2 -DNDEBUG'
if [ ! -e "$ihme_data_drive/Project" ]
then
	ihme_data_drive=''
fi
# ------------------------------------------------------------------------
# print parameters here
cat << EOF
These parameters must be set so they correspond to your system.

gsl_prefix=$gsl_prefix
ihme_data_drive=$ihme_data_drive
dismod_ode_prefix=$dismod_ode_prefix
python_program=$python_program
cmake_build_type=$cmake_build_type
EOF
# ------------------------------------------------------------------------
# These parameters correspond to this distriubtion of dismod_ode
dismod_ode_version='20160713'
cppad_version='20160000.1'
# ------------------------------------------------------------------------
local_prefix=`pwd`'/prefix'
if [ -e 'build' ]
then
	rm -r build
fi
mkdir build
cd build
#
web_page=http://www.coin-or.org/download/source/CppAD
wget $web_page/cppad-$cppad_version.gpl.tgz
tar -xzf cppad-$cppad_version.gpl.tgz
cd cppad-$cppad_version
mkdir build
cd build
cmake -D cppad_prefix=$local_prefix ..
make install
cd ../..
#
if [ ! -e "$local_prefix/include/cppad" ]
then
	echo 'cppad local install failed'
	exit 1
fi
cmake \
	-D dismod_ode_prefix=$dismod_ode_prefix \
	-D cppad_prefix=$local_prefix \
	-D gsl_prefix=$gsl_prefix \
	-D python_program=$python_program \
	-D ihme_data_drive=$ihme_data_drive \
	-D cxx_flags="$cxx_flags" \
	-D CMAKE_BUILD_TYPE=None \
	..
#
echo 'setup.sh: OK'

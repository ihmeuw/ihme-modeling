Setting Up
==========


At this time it is required that the Dalynator/Burdenator be installed into a
`conda environment <https://conda.io/docs/using/envs.html>`_ to avoid conflicts
with other packages you might be using in your other day-to-day work.

We'll be installing a few dependencies from an internal IHME PyPi server. To
use it, you'll need to add these lines to the file ``~/.pip/pip.conf`` (if the
file does not exist, go ahead and create it; note: ``~`` is your linux home directory, i.e. ``FILEPATH``
aka your "FILEPATH")::

    [global]
    extra-index-url = ADDRESS
    trusted-host = ADDRESS

If you don't already have conda installed into your home directory, get
yourself a **qlogin with 2 cores (the following must all be performed within the context of
qlogin)** and follow these instructions: `Linux Miniconda Install
<https://conda.io/docs/install/quick.html#linux-miniconda-install>`_

Got conda? Great, now you'll want to create a conda environment where the
DALYnator/Burdenator can live and activate it::

    conda create -n dalynator python=3
    source activate dalynator

If all went well, your command prompt should be prefixed with the tag
*(dalynator)*. If something went poorly, you might have received a message
like "CondaValueError." Unfortunately, if this is the case, you may need to
check that you have properly installed conda using the links above.

Next, you'll want to install the DALY/Burdenator and its
dependencies. You'll have to clone and install the DALY/Burdenator
from Bitbucket (Stash) at this time, as we haven't gotten it on a proper
container system yet.  Sorry. We'll fix that someday. To clone and install::

    git clone ADDRESS
    cd dalynator
    conda install --file conda_requirements.txt
    pip install -r requirements.txt
    pip install .

Assuming that went well (i.e. pip didn't throw any big red error messages), you
should be ready to launch either application.

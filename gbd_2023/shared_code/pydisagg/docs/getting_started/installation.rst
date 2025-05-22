===================
Installing pyDisagg
===================

Python
------

The package :code:`pydisagg` is written in Python
and requires Python 3.8 or later.

:code:`pydisagg` package is distributed at
`PyPI <https://pypi.org/project/pydisagg/>`_.
To install the package:

.. code::

   pip install pydisagg

For developers, you can clone the repository and install the package in the
development mode.

.. code::

    git clone https://github.com/ihmeuw-msca/pyDisagg.git
    cd pyDisagg
    pip install -e ".[test,docs]"


R
-
Install reticulate if it isn't already

.. code-block:: r

   install.packages("reticulate")

It is encouraged to create a virtual environment for pyDisagg

.. code-block:: r

   library(reticulate)
   use_condaenv("your_conda_env")
   py_install("pydisagg")



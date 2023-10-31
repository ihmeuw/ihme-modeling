Covid Model SEIIR Pipeline
==========================

.. note::

   This package has not yet been configured for use outside the IHME
   infrastructure.  We're working on it! Feel free to submit questions and
   issues using the standard github tools, and we'll respond as soon as we
   can.

Installation on the IHME cluster
--------------------------------

To install this package on the IHME cluster, first you should create and activate a new
`conda <https://docs.conda.io/en/latest/>`_ environment:

.. code-block:: bash

   $> conda create --name=seir python=3.8
   $> conda activate seir

You can then clone and install this repository:

.. code-block:: bash

   $> git clone https://github.com/ihmeuw/covid-model-seiir-pipeline.git
   $> cd covid-model-seiir-pipeline
   $> pip install .[internal]

The above example uses https for cloning the repository, but you can also clone with
ssh or the `github cli <https://cli.github.com/>`_. Note that we are installing the extra
`internal` dependencies for the package, including `jobmon` for running workflows and
`db_queries` for accessing location hierarchy information.

Running Models
--------------

.. todo::

   This section.

Contributing
------------

If you would like to contribute to this project, please start by reading our
`Guide to Contributing <CONTRIBUTING.rst>`_. Please note that this project is released
with a `Contributor Code of Conduct <CODE_OF_CONDUCT.rst>`_. By participating in this
project you agree to abide by its terms.

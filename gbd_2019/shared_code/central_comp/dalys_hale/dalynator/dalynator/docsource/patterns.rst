
Computation Element
===================

The design principle is that each Class should do one thing. Every class that performs a GBD computation
is a subclass of ``ComputationElement.``  The  ``__init__`` method records all the parameters for the task,
then get_data_frame() executes this Computation Elment.
For example, see ComputeDalys, which has the  DALY = YLL + YLD function.

A ComputationElement operates over one or more DataFrames, and produces one or more DataFrames.

DataSources are special ComputationElements that produce DataFrames from something other than a DataFrame,
typically from files or the database. For example, the ``SqlDataSource`` Class reads from the database and creates a DataFrame.
DataSinks writes a DataFrame out to a different format, typically H5 (``HDFDataSink1``) and CSV files.

The ``run_pipeline`` function strings together all the ComputationElements that make up the "Dalynator". At present the
pipelines are not ComputationElements, but we feel that the code would be clearer if there was some sort of structure
around these functions.

``run_all-dalynator.py`` is the entry point - it accepts a wide variety of arguments and then qsubs individual pipeline executions.

API reference
=============

.. toctree::
   :hidden:
   :glob:

   *

.. note::
   Briefly describe the organization of the API reference if any.


Age Splitting
-------------

AgeDataConfig
-------------

.. py:class:: AgeDataConfig(BaseModel)

   Configuration for age data, including column names for index, age lower and upper bounds, value, and standard deviation.

   .. py:attribute:: index
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: age_lwr
      :type: str

      The column name for the lower bound of age.

   .. py:attribute:: age_upr
      :type: str

      The column name for the upper bound of age.

   .. py:attribute:: val
      :type: str

      The column name for the value.

   .. py:attribute:: val_sd
      :type: str

      The column name for the standard deviation of the value.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names, ensuring uniqueness.

AgePopulationConfig
-------------------

.. py:class:: AgePopulationConfig(BaseModel)

   Configuration for population data, including index and value columns, with support for prefix application.

   .. py:attribute:: index
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: val
      :type: str

      The column name for the value.

   .. py:attribute:: prefix
      :type: str
      :value: "pop_"

      The prefix to apply to the value field.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names including the index and value.

   .. py:property:: val_fields
      :type: list[str]

      Returns a list of fields that contain values.

   .. py:method:: apply_prefix()

      Applies the prefix to value fields and returns a mapping of original to new field names. The method returns a dictionary where keys are the original field names and values are the new field names with prefixes applied.

   .. py:method:: remove_prefix()

      Removes the prefix from value fields.

AgePatternConfig
----------------

.. py:class:: AgePatternConfig(BaseModel)

   Configuration for age pattern data, including index, age bounds, value, standard deviation, and support for prefix application.

   .. py:attribute:: by
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: age_key
      :type: str

      The column name for the age key.

   .. py:attribute:: age_lwr
      :type: str

      The column name for the lower bound of age.

   .. py:attribute:: age_upr
      :type: str

      The column name for the upper bound of age.

   .. py:attribute:: draws
      :type: list[str]
      :value: []

      A list of column names for draws.

   .. py:attribute:: val
      :type: str
      :value: "val"

      The column name for the value.

   .. py:attribute:: val_sd
      :type: str
      :value: "val_sd"

      The column name for the standard deviation of the value.

   .. py:attribute:: prefix
      :type: str
      :value: "pat_"

      The prefix to apply to value fields.

   .. py:property:: index
      :type: list[str]

      Returns a list of all column names used as the index.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names including the index, age bounds, value, and standard deviation.

   .. py:property:: val_fields
      :type: list[str]

      Returns a list of fields that contain values.

   .. py:method:: apply_prefix()

      Applies the prefix to value fields and returns a mapping of original to new field names. The method returns a dictionary where keys are the original field names and values are the new field names with prefixes applied.

   .. py:method:: remove_prefix()

      Removes the prefix from value fields.

AgeSplitter
------------

.. py:class:: AgeSplitter(data: AgeDataConfig, pattern: AgePatternConfig, population: AgePopulationConfig)

   This class is responsible for splitting age data based on specified patterns and population configurations.

   .. py:method:: model_post_init(__context: Any) -> None

      Performs extra validation of all the indexes after model initialization.

      :param __context: Any context needed for post-initialization.
      :raises ValueError: If any of the validation checks fail.

   .. py:method:: parse_data(data: DataFrame, positive_strict: bool) -> DataFrame

      Parses and validates the input data according to the specified data configuration.

      :param data: The input data to parse.
      :param positive_strict: Whether to strictly validate positive values.
      :return: The validated and parsed data.

   .. py:method:: parse_pattern(data: DataFrame, pattern: DataFrame, positive_strict: bool) -> DataFrame

      Parses and validates the input pattern data according to the specified pattern configuration.

      :param data: The base data to which the pattern will be applied.
      :param pattern: The pattern data to parse.
      :param positive_strict: Whether to strictly validate positive values.
      :return: The data with the pattern applied.

   .. py:method:: parse_population(data: DataFrame, population: DataFrame) -> DataFrame

      Parses and validates the input population data according to the specified population configuration.

      :param data: The base data to which the population will be applied.
      :param population: The population data to parse.
      :return: The data with the population applied.

   .. py:method:: split(data: DataFrame, pattern: DataFrame, population: DataFrame, model: str = "rate", output_type: str = "rate", propagate_zeros: bool = False) -> DataFrame

      Splits the data based on the given pattern and population. The split results are added to the data as new columns.

      :param data: The data to be split.
      :param pattern: The pattern to be used for splitting the data.
      :param population: The population to be used for splitting the data.
      :param model: The model to be used for splitting the data, by default "rate". Can be "rate" or "logodds".
      :param output_type: The type of output to be returned, by default "rate".
      :param propagate_zeros: Whether to propagate pre-split zeros as post-split zeros. Default false.
      :return: The data with split results added as new columns.

Sex Splitting
-------------


SexDataConfig
-------------

.. py:class:: SexDataConfig(BaseModel)

   Configuration for sex data, including column names for index, sex categories, value, and standard deviation.

   .. py:attribute:: index
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: sex
      :type: str

      The column name for the sex categories.

   .. py:attribute:: val
      :type: str

      The column name for the value.

   .. py:attribute:: val_sd
      :type: str

      The column name for the standard deviation of the value.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names, ensuring uniqueness.

SexPopulationConfig
-------------------

.. py:class:: SexPopulationConfig(BaseModel)

   Configuration for population data, including index and value columns, with support for prefix application.

   .. py:attribute:: index
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: sex
      :type: str

      The column name for the sex categories.

   .. py:attribute:: sex_m
      :type: str | int

      The identifier for male sex category.

   .. py:attribute:: sex_f
      :type: str | int

      The identifier for female sex category.

   .. py:attribute:: val
      :type: str

      The column name for the value.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names including the index and value.

   .. py:property:: val_fields
      :type: list[str]

      Returns a list of fields that contain values.

   .. py:method:: apply_prefix()

      Applies the prefix to value fields and returns a mapping of original to new field names. The method returns a dictionary where keys are the original field names and values are the new field names with prefixes applied.

   .. py:method:: remove_prefix()

      Removes the prefix from value fields.

SexPatternConfig
----------------

.. py:class:: SexPatternConfig(BaseModel)

   Configuration for sex pattern data, including index, sex categories, value, standard deviation, and support for prefix application.

   .. py:attribute:: by
      :type: list[str]

      A list of column names used as the index.

   .. py:attribute:: draws
      :type: list[str]
      :value: []

      A list of column names for draws.

   .. py:attribute:: val
      :type: str
      :value: "ratio_f_to_m"

      The column name for the value.

   .. py:attribute:: val_sd
      :type: str
      :value: "ratio_f_to_m_se"

      The column name for the standard deviation of the value.

   .. py:attribute:: prefix
      :type: str
      :value: "sex_pat_"

      The prefix to apply to value fields.

   .. py:property:: index
      :type: list[str]

      Returns a list of all column names used as the index.

   .. py:property:: columns
      :type: list[str]

      Returns a list of all column names including the index, sex categories, value, and standard deviation.

   .. py:property:: val_fields
      :type: list[str]

      Returns a list of fields that contain values.

   .. py:method:: apply_prefix()

      Applies the prefix to value fields and returns a mapping of original to new field names. The method returns a dictionary where keys are the original field names and values are the new field names with prefixes applied.

   .. py:method:: remove_prefix()

      Removes the prefix from value fields.

SexSplitter
------------

.. py:class:: SexSplitter(data: SexDataConfig, pattern: SexPatternConfig, population: SexPopulationConfig)

   This class is responsible for splitting sex data based on specified patterns and population configurations.

   .. py:method:: model_post_init(__context: Any) -> None

      Performs extra validation of all the indexes after model initialization.

      :param __context: Any context needed for post-initialization.
      :raises ValueError: If any of the validation checks fail.

   .. py:method:: parse_data(data: DataFrame) -> DataFrame

      Parses and validates the input data according to the specified data configuration.

      :param data: The input data to parse.
      :return: The validated and parsed data.

   .. py:method:: parse_pattern(data: DataFrame, pattern: DataFrame) -> DataFrame

      Parses and validates the input pattern data according to the specified pattern configuration.

      :param data: The base data to which the pattern will be applied.
      :param pattern: The pattern data to parse.
      :return: The data with the pattern applied.

   .. py:method:: parse_population(data: DataFrame, population: DataFrame) -> DataFrame

      Parses and validates the input population data according to the specified population configuration.

      :param data: The base data to which the population will be applied.
      :param population: The population data to parse.
      :return: The data with the population applied.

   .. py:method:: split(data: DataFrame, pattern: DataFrame, population: DataFrame, model: str = "rate", output_type: str = "rate") -> DataFrame

      Splits the data based on the given pattern and population. The split results are added to the data as new columns.

      :param data: The data to be split.
      :param pattern: The pattern to be used for splitting the data.
      :param population: The population to be used for splitting the data.
      :param model: The model to be used for splitting the data, by default "rate". Can be "rate" or "logodds".
      :param output_type: The type of output to be returned, by default "rate".
      :return: The data with split results added as new columns.

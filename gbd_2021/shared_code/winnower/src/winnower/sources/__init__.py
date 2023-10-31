from . import (
    dbf,
    excel,
    separated_values,
    stata,
    spss,
    xport,
)

from winnower import errors
from winnower.globals import eg
from winnower.logging import (
    get_class_logger,
    log_notimplemented,
)
from winnower.util.dtype import is_string_dtype


@log_notimplemented
def get_columns(path, *, delimiter=None):
    suffix = path.suffix.lower()
    if suffix == '.dta':
        return stata.get_stata_columns(path)
    elif suffix in {'.xls', '.xlsx'}:
        return excel.get_excel_columns(path)
    elif suffix in {'.csv', '.tab', '.txt'}:
        delimiter = _resolve_delimiter(delimiter, suffix)
        return separated_values.get_separated_values_columns(
            path, delimiter=delimiter)
    elif suffix == '.dbf':
        return dbf.get_dbf_columns(path)
    elif suffix == '.sav':
        return spss.get_spss_columns(path)
    elif suffix == '.xpt':
        return xport.get_xport_columns(path)
    else:
        raise NotImplementedError("{} not supported".format(suffix))


@log_notimplemented
def get_dataframe(path, *, delimiter=None, columns=None):
    # diabling efficient column loading temporarily
    # For now, setting columns=None will load whole data.
    columns = None

    suffix = path.suffix.lower()
    if suffix == '.dta':
        return stata.load_stata(path, columns)
    elif suffix in {'.xls', '.xlsx'}:
        return excel.load_excel(path)
    elif suffix in {'.csv', '.tab', '.txt'}:
        delimiter = _resolve_delimiter(delimiter, suffix)
        return separated_values.load_separated_values(
            path, delimiter=delimiter, usecols=columns)
    elif suffix == '.dbf':
        return dbf.load_dbf(path)
    elif suffix == '.sav':
        return spss.load_spss(path, usecols=columns)
    elif suffix == '.xpt':
        return xport.load_xport(path, usecols=columns)
    else:
        raise NotImplementedError("{} not supported".format(suffix))


def _resolve_delimiter(delimiter, suffix):
    # mandatory delimiter for text files
    if suffix == '.txt':
        delimiter = ' '
    elif delimiter is None:
        if suffix == '.csv':
            delimiter = ','
        elif suffix == '.tab':
            delimiter = '\t'
        else:
            msg = f"Unknown what delimiter to use for extension {suffix}"
            raise ValueError(msg)

    return delimiter


# Used for merging one dataframe with another using the Merge transform but
# this should be generally useful and has an equivalent method set to
# FileSource.
class DataFrameSource:
    def __init__(self, dataframe, *, keep=None):
        self.dataframe = dataframe
        self.keep = keep
        self._metadata = None
        self._output_columns = None
        self._column_labels = None
        self.logger = get_class_logger(self)
        self.uses_columns = None

    def input_columns(self):
        """
        Just the dataframes columns
        """
        return self.dataframe.columns

    # Similar API to winnower.transform.base.Component - but this takes no arg
    def output_columns(self):
        if self._output_columns is None:
            if not self.keep:
                self._output_columns = self.dataframe.columns
            else:
                self._output_columns = [x for x in self.dataframe.columns
                                        if x in self.keep]
        return list(self._output_columns)

    def validate(self):  # TODO: does not match component.validate() arity.
        pass
        # TODO: anything else to validate?
        # try and open the file (validation could get 'spensive...)

    @log_notimplemented
    def execute(self):
        df = self.get_dataframe()
        self._metadata = "Add metadata"  # TODO:
        return df

    def execute_metdata(self):
        # TODO: consider raising error
        return self._metadata

    def get_dataframe(self):
        if not self.keep:
            # TODO: consider making deep an option
            return self.dataframe.copy(deep=True)
        # otherwise return only the columns kept.
        return self.dataframe[self.keep].copy()

    def get_value_labels(self):
        """
        Return value labels ...

        Should this return the categorical labels?

        Returns value_labels, a dict of dicts.

        value_labels[column_name][value_in_column] == 'label for value'
        """
        return {}

    def get_column_labels(self):
        """
        Returns column_labels, a dict.

        column_labels[column_name] == 'label for column'
        """
        if self._column_labels is None:
            self._column_labels = {col_name: col_name
                                for col_name in self.output_columns()}

        return self._column_labels.copy()


class FileSource:
    def __init__(self, path, *, delimiter=None):
        self.path = path
        self.delimiter = delimiter
        self._metadata = None
        self._output_columns = None
        self._column_labels = None
        self.logger = get_class_logger(self)
        self.uses_columns = None

    def input_columns(self):
        """
        FileSource's have no input columns.
        """
        return []

    # Similar API to winnower.transform.base.Component - but this takes no arg
    def output_columns(self):
        if self._output_columns is None:
            # TODO: consider retaining the open file
            self._output_columns = get_columns(self.path,
                                               delimiter=self.delimiter)
        return list(self._output_columns)

    def validate(self):  # TODO: does not match component.validate() arity.
        if not self.path.exists():
            msg = "File {} does not exist".format(self.path)
            raise errors.ValidationError(msg)

    @log_notimplemented
    def execute(self):
        df = self.get_dataframe()
        if not eg.strict:
            # strip all leading/trailing spaces from str columns
            obj_columns = df.select_dtypes(include='object').columns
            for col_name in obj_columns:
                if col_name.lower() == 'vcal_1': # NOTE need to know blanks for sbh stillbirth calculation
                    self.logger.debug("SKIPPING space-stripping logic for 'vcal_1' column")
                    continue
                col = df[col_name]
                if is_string_dtype(col):
                    try:
                        df[col_name] = col.str.strip()
                        self.logger.info(f"Stripping spaces from column {col}")
                    except Exception:
                        self.logger.info("Failed to strip spaces from column "
                                        f"{col} using original values instead")
        return df

    def execute_metdata(self):
        # TODO: consider raising error
        return self._metadata

    def get_dataframe(self):
        return get_dataframe(self.path,
                             delimiter=self.delimiter,
                             columns=self.uses_columns)

    def get_value_labels(self):
        """
        Return value labels embedded in data file.

        Only returns data for labeled columns.

        Returns value_labels, a dict of dicts.

        value_labels[column_name][value_in_column] == 'label for value'
        """
        if self.path.suffix.lower() == '.dta':
            return stata.get_stata_value_labels(self.path)
        elif self.path.suffix.lower() == '.sav':
            return spss.get_spss_value_labels(self.path)
        else:
            return {}

    def get_column_labels(self):
        """
        Returns variable labels embedded in data file.

        Returns column_labels, a dict.

        column_labels[column_name] == 'label for column'
        """
        if self._column_labels is None:
            if self.path.suffix.lower() == '.dta':
                self._column_labels = stata.get_stata_column_labels(self.path)
            elif self.path.suffix.lower() == '.sav':
                self._column_labels = spss.get_spss_column_labels(self.path)
            else:
                self._column_labels = {col_name: col_name
                                    for col_name in self.output_columns()}

        return self._column_labels.copy()

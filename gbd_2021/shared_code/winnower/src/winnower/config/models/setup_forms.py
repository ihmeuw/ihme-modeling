"""
Form processors for the "setup" databases.
"""
import attr

from .fields import (
    required_attrib,
    optional_attrib,
    required_shared_filesystem_path,
    space_separated_values,
    comma_separated_values,
    pandas_bool,
    pandas_int,
    pandas_str,
    separated_values,
    optional_varlist,
    nan_or,
    winnower_id_field,
)

# Classes holding row-level configuration data (but not always the whole row)
@attr.s
class UniversalForm:
    """
    A single survey with related configuration.

    Args:
        ubcov_id: the primary identifier for matching survey-specific config.
        nid: node id per GHDx.
        survey_name: the name of the survey file. Based off of the name of the
            folder path in J:/DATA
        ihme_loc_id: IHME location id for the region the survey has data for.
        year_start: the earliest year the survey provides data for.
        year_end: the latest year the survey provides data for.
        survey_module: refers to the logical grouping of information the survey
            provides, and categorized by the GHDx. See also:

            <ADDRESS>

            under "File description".
        file_path: Windows-based file path. Use / instead of \\ for separator.
        reshape: space or comma delimited list of columns to reshape survey
            data usually to facilitate a join with another dataset.
        reshape_stem (bool): If True rename columns ending in 2 digits by
            inserting _ before the last two digits. e.g., "foo42" => "foo_42".
        subset: string command to be eval()'d. Normally this subsets (filters)
            the dataset to a smaller form with only records of interest.
            However in some cases this does other things. Examples:
                decode: take a numeric column with value labels and create a
                    string column containing those decoded values.
                generate: create a new column using an expression.
                rename: rename a column.
        delimiter: if provided, and file_path has a '.csv' or '.tab' extension,
            loads file using that delimiter. If not provided, .csv and .tab
            files are loaded and the delimiter is auto-detected as one of
            '\t' or ','.

     Notes:

    A single survey may be distributed in several files. In this case they will
    share a "record nid" even though they will have individual "file nid"s.

    Some survey files provide data for:
        * multiple ihme_loc_id's.
        * years outside of the specified year_start, year_end range.

    In this case the year-specific data is isolated through the `subset`
    directive in "basic" configuration.
    """
    ubcov_id = required_attrib(converter=int)
    winnower_id = winnower_id_field()
    nid = required_attrib(converter=int)
    survey_name = required_attrib()
    ihme_loc_id = required_attrib()  # could be validated
    year_start = required_attrib(converter=int)
    year_end = required_attrib(converter=int)
    survey_module = required_attrib()
    file_path = required_shared_filesystem_path()
    # inputs should be blank (NaN) or 1.
    reshape = optional_attrib(converter=separated_values)
    reshape_keepid = optional_attrib(converter=comma_separated_values)
    # inputs should be blank (NaN) or 1.
    reshape_stem = optional_attrib(converter=pandas_bool)
    subset = optional_attrib()
    delimiter = optional_attrib(converter=pandas_str)


@attr.s
class MergeForm:
    """
    Allows mergeing (joining) of multiple files into a single dataset.

    When merging two files we will use the term "master" to refer to the data
    set supplied by the UniversalForm and "using" to refer to the data set
    supplied by the MergeForm. These terms are borrowed from Stata.

    Args:
        ubcov_id: identifier for the corresponding "Universal Form" row.
        topic_name: name of the topic this merge pertains to.
        merge_file: windows-based path to the file to merge.
        master_vars: comma-separated list of vars in the master data set to
            merge on.
        using_vars: comma-separated list of vars in the using data set to
            merge on. Must be the same length as master_vars.
        type: type of merge; one of ("1:1", "m:1", "1:m", "m:m").
        keep: comma or space separated list of int values 1-3. Each value
            indicates a subset of records which should be kept in the merge.

            1 - records which contain data from master.
            2 - records containing data from using
            3 - records containing data from both master and using.
        reshape: space or comma delimited list of columns to reshape survey
            data usually to facilitate a join with another dataset.
    """
    winnower_id = winnower_id_field()
    file_path = required_shared_filesystem_path()
    topic_name = required_attrib()
    merge_file = required_shared_filesystem_path()
    master_vars = required_attrib(converter=separated_values)
    using_vars = required_attrib(converter=separated_values)
    type = required_attrib()

    nid = required_attrib(converter=int)
    ihme_loc_id = required_attrib()  # could be validated
    year_start = required_attrib(converter=int)
    year_end = required_attrib(converter=int)
    survey_module = required_attrib()

    ubcov_id = optional_attrib(converter=pandas_int)
    keep = optional_attrib(converter=separated_values)
    reshape = optional_attrib(converter=space_separated_values)


@attr.s
class LabelForm:
    """
    Configuration for labels.

    The Labels database supports two logical entities - variable labels and
    value labels.

    A variable label is defined by the attributes
        file_path
        variable
        variable_label

    Note that variable labels are not yet supported.

    A value label is defined by the attributes
        file_path
        variable
        value_num
        value_str

    Note that for a given dataset (file_path) and column (variable) there will
    likely be a distinct value_num/value_str mapping pair for each unique value
    in the column.

    Value labels perform a value mapping for source files. These mapping
    replace values with new values so that e.g., 1 becomes 'PHYSICIAN', 2
    becomes 'NURSE', etc.
    """
    # TODO: support value labels (add "variable_label") and predicate method
    file_path = required_shared_filesystem_path()
    variable = required_attrib()
    variable_label = optional_attrib()
    value_num = optional_attrib(converter=pandas_int)
    value_str = optional_attrib()

    def is_value_label(self):
        return self.value_num is not None and self.value_str is not None

    def is_column_label(self):
        return self.variable is not None and self.variable_label is not None


@attr.s
class IndicatorForm:
    """
    An indicator is an output column in our extraction.

    Args:
        indicator_name: name of indicator.
        topic_name: the topic the indicator belongs to.
        indicator_type: description of indicator such as "meta_num" or "bin".
        input_vars: the str column named used to inform this indicator.
        input_meta: contextually dependant on indicator_type - see below.
        map_indicator (bool): indicates that the indicator has topic-specific
            value maps assigned to it.
        code_custom (bool): indicates this field is populated by custom code.
            All other config is essentially meaningless.
        indicator_required (bool): if True, error if this indicator cannot be
            generated.
    """
    indicator_name = required_attrib()
    topic_name = required_attrib()
    indicator_type = required_attrib()
    input_vars = optional_attrib()
    input_meta = optional_varlist(
        converter=nan_or(separated_values, default=()))
    map_indicator = optional_attrib(converter=pandas_bool)
    code_custom = optional_attrib(converter=pandas_bool)
    indicator_required = optional_attrib(converter=pandas_bool)


@attr.s
class VarForm:
    """
    Configuration regarding input variables used for processing topics.
    """
    var_name = required_attrib()
    topic_name = required_attrib()

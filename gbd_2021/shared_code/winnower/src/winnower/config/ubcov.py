"""
For loading CSV-based configuration from the original ubCov implementation.

This will support two flavors of CSV

* Plain .csv files used for testing. Also the geography codbooks are CSV files
* Google Spreadsheets exported as CSV - most ubCov configuration lives online
  at sheets.google.com. It is most easily exported as a CSV file.

Most of interest in this module is the UbcovConfigLoader.
"""
from __future__ import generator_stop
# TODO: consider defining __all__ = ['UbcovConfigLoader', ...]
from argparse import Namespace
from collections import defaultdict
from functools import partial
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from contextlib import contextmanager
import importlib
import itertools
import re
import string
import sys
import warnings
import hashlib

import attr
import pandas

from winnower import constants
from winnower.custom.base import (
    ExtractionMetadata,
)
from winnower import errors
from winnower.extract import (
    ExtractionChain,
    ValidatedExtractionChain,
    ExtractionChainBuilder,
    get_merges_from_chain,
)
from winnower.extract_hooks import (
    get_hooks,
)
from winnower.logging import (
    get_class_logger,
    log_notimplemented,
)
from winnower.sources import FileSource
from winnower.transform import (
    ConstantColumn,
    DropEmptyRows,
    KeepColumns,
    MapIndiaUrbanRuralSubnationals,
    MapNumericValues,
    MapSubnationals,
    MapValues,
    Merge,
    RenameColumn,
    Reshape,
    Subset,

    STRICT_GBD_SUBNAT_LOCS,  # used to restrict winnower to ubcov behavior.
    transform_from_indicator,
    is_CategoricalStr_indicator,
)
from winnower.util.path import (
    resolve_path_case,
    fix_survey_path,
    strip_root_and_standardize,
)
from winnower.util.dataframe import rows_with_nulls_mask
from .models import attr_items, get_form, required_fields, setup_forms
from winnower.util.categorical import downcast_float_to_integer

# Used in creating "global" session and extraction contexts.
from winnower.ctx import (
    WinnowerExtractionContext,
)

from winnower.globals import eg  # Session globals and extraction globals.

# We intentionally supply a generic dtype with a more specific converter
warnings.filterwarnings(
    action='ignore',
    category=pandas.errors.ParserWarning,
    message=('Both a converter and dtype were specified for column ubcov_id - '
             'only the converter will be used'))


# Methods
def get_configuration_paths(root):
    """
    Returns configuration paths object given root.

    Args:
        root: str or pathlib.Path.
            str: URL referring to a Google Sheet containing "links".
            Path: local filesystem path containing CSV files.
    """
    if isinstance(root, Path):
        return csv_paths(root)
    elif isinstance(root, str):
        return UrlPaths.url_links_map(root)


def fix_basic_codebook(paths):
    """
    Change location of "basic" from a codebook to a top-level entity.

    Basic is not a regular codebook.

    Mutates `paths` in-place.
    """
    # DRY method
    paths['basic'] = paths['codebooks'].pop('basic')


def fix_basic_additional_codebook(paths):
    """
    Change location of "basic_additional" from codebook to top-level entity
    """
    paths['basic_additional'] = paths['codebooks'].pop('basic_additional')


def topic_name(path):
    """Returns the topic for a given path."""
    # Due to the way the spreadsheets are exported to CSV, this is just the
    # name of the parent folder.
    return path.parent.name


def csv_paths(root):
    """
    Returns dict of paths to CSV files relative to `root` directory.
    """
    if not root.exists():
        raise ValueError(f"Path {root!r} doesn't exist. Can't determine paths")

    paths = {
        # NOTE: keys are based off of "obj" field in links file,
        # which normally pluralizes things
        'gbd_subnat_map': root / 'gbd_subnat_map' / 'Sheet1.csv',
        'merges': root / 'merges' / 'Sheet1.csv',
        'labels': root / 'labels' / 'Sheet1.csv',
        'indicators': root / 'indicators' / 'Sheet1.csv',
        'topic': root / 'topic' / 'Sheet1.csv',
        'vars': root / 'vars' / 'Sheet1.csv',
    }

    value_maps = root / 'value_map'
    paths['value_maps'] = {topic_name(X): X
                           for X in value_maps.glob('*/Sheet1.csv')}
    codebooks = root / 'codebook'
    paths['codebooks'] = {topic_name(X): X
                          for X in codebooks.glob('*/sheet1.csv')}
    # 1-offs:
    fix_basic_codebook(paths)
    fix_basic_additional_codebook(paths)

    return paths


class UrlPaths:
    """
    Returns a dict of urls specified in `root_url` providing CSV resources.

    root_url refers to the "Links" spreadsheet. This spreadsheet contains
    3 columns - obj, topic, key. This sheet provides the "key" necessary to
    generate working URLs to other ubCov configuration files.

    This is the mechanism ubCov uses internally to discover configuration
    spreadsshets.
    """
    url_pattern = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'  # noqa

    @classmethod
    def url_links_map(cls, root_url):
        links_df = pandas.read_csv(root_url)

        result = {}
        value_maps = result['value_maps'] = {}
        codebooks = result['codebooks'] = {}

        for nt in links_df.itertuples():
            url = cls.url_for(nt.key)

            obj = nt.obj
            topic = nt.topic

            if pandas.isnull(topic):
                result[nt.obj] = url
            else:
                if obj == 'codebook':
                    codebooks[topic] = url
                elif obj == 'value_map':
                    value_maps[topic] = url
                else:
                    msg = f"Unknown obj {obj} (topic {topic})"
                    raise NotImplementedError(msg)

        fix_basic_codebook(result)
        fix_basic_additional_codebook(result)
        return result

    @classmethod
    def url_for(cls, key):
        return cls.url_pattern.format(key=key)

    @classmethod
    def for_google_docs(cls):
        return cls.url_for(constants.GDOCS_LINKS_KEY)


# Classes for loading configuration data
class UbcovConfigLoader:
    """
    Loads ubCov configuration from CSV-like files.
    """
    setup_form_classes = {
        'indicators': setup_forms.IndicatorForm,
        'labels': setup_forms.LabelForm,
        'merges': setup_forms.MergeForm,
        'universal': setup_forms.UniversalForm,
        'vars': setup_forms.VarForm,
    }

    @classmethod
    def from_root(cls, root, strict=False):
        """
        Factory creating a configuration loader.

        Args:
            root: str or Path to the "root" of the ubCov configuration.
                str: URL referring to a Google Sheet containing "links". Links
                    contains keys which can be used to assemble URL's to all
                    other sheets.
                Path: local filesystem path containing CSV files. These will be
                    stored hierarchically and generated by the ubcov_backup
                    project.
            strict: bool, indicates if winower should run in strict mode.
                    When in strict mode winnower attempts to faithfully
                    recreate ubcov's extraction behavior even when that
                    behavior is known to be incorrect.

        ubcov_backup source:
        <FILEPATH>
        """
        # TODO: errors???
        paths = get_configuration_paths(root)
        result = cls(paths, strict=strict)
        result.initialize()
        return result

    def get_extractor(self, ubcov_id, topics=()):
        """
        Load all of the dbs and necessary info for the corresponding ubcov_id
        """
        # Some things need the 'basic' topic, while others don't
        self._ensure_basic_loaded()
        universal_tf = self.get_tableframe('universal')
        # id-specific
        # get basic (SPLIT - Geog, Demographics, Design, Universal, Survey)
        universal = universal_tf.get(ubcov_id=ubcov_id)
        self.logger.debug(f"universal config {universal}")
        # get merges
        self._ensure_merges_loaded()
        merge_tf = self.get_tableframe('merges')
        # Filter merges on winnower_id so that only files included
        # in a particular survey extraction are merged. Winnower_id is
        # generated from a hash of the IHME survey key (constants.SURVEY_KEY)
        # which together should uniquely identify a survey.
        #
        # m:m merges are performed differently in ubcov and winnower
        # Ubcov does not do a full outer join on the left and right df
        # while winnower does. m:m merges done by ubcov will have fewer rows
        # than winnower if both the left and right df have duplicates. For
        # more information see merge.py in transforms.
        #
        all_merges = merge_tf.filter(file_paths=[universal.file_path],
                                     topics=('basic',) + topics)
        merges = tuple(M for M in all_merges
                       if all(getattr(M, key) == getattr(universal, key)
                              for key in constants.MERGE_KEY))
        self.logger.debug(f"merges config {merges}")
        # get gbd_subnat_map string matches for the relevant loc_id
        ihme_loc_id = universal.ihme_loc_id

        d_msg = (f'Attempting to gather gbd_subnat_map for ihme_loc_id: '
                 f'{ihme_loc_id}.')
        self.logger.debug(d_msg)

        # Here, the strict value informs which ihme_loc_ids are valid gbd
        # subnational locals. Ubcov has a hardcoded set of subnational locals.
        if self.strict and not (ihme_loc_id in STRICT_GBD_SUBNAT_LOCS):
            gbd_subnat_map = None
            msg = (f'STRICT: Could not find ihme_loc_id: {ihme_loc_id} in '
                   'the STRICT_GBD_SUBNAT_LOCS list. GBD subnational '
                   'mapping will not be done.')
            self.logger.warning(msg)

            d_msg = f'STRICT_GBD_SUBNAT_LOCS: {STRICT_GBD_SUBNAT_LOCS}'
            self.logger.debug(d_msg)
        else:
            self._ensure_gbd_subnat_map_loaded()
            gbd_subnat_map = self.data_frames['gbd_subnat_map']
            loc_ids_with_subnat_map = gbd_subnat_map['ihme_loc_id'].unique()
            if ihme_loc_id not in loc_ids_with_subnat_map:
                gbd_subnat_map = None
                msg = (f'Could not find ihme_loc_id: {ihme_loc_id} in '
                       'the gbd_subnat config. GBD subnational '
                       'mapping will not be done.')
                self.logger.info(msg)

                d_msg = f'loc_ids_with_subnat_map: {loc_ids_with_subnat_map}'
                self.logger.debug(d_msg)

        if gbd_subnat_map is not None:
            mask = gbd_subnat_map['ihme_loc_id'] == ihme_loc_id
            cols_to_keep = ['indicator',
                            'value',
                            'location_name_short_ihme_loc_id']
            gbd_subnat_map = gbd_subnat_map.loc[mask, cols_to_keep]

        # get value maps for the relevant topics
        topic_value_maps = self.get_value_maps(topics)

        # FILE-specific
        # get labels for basic and merges
        label_paths = [universal.file_path]
        label_paths.extend(X.merge_file for X in merges)
        value_labels, column_labels = self._get_label_dicts(label_paths)

        # TOPIC-specific
        topic_config = {}
        for topic_name in topics:
            # maps
            # topic specific config (codebooks)
            topic_tf = self.get_tableframe(topic_name, type='codebooks')
            topic_config[topic_name] = topic_tf.get(ubcov_id=ubcov_id)

        # vars
        vars_tf = self.get_tableframe('vars')
        vars = tuple(vars_tf.filter(topics=topics))

        # indicators
        indicators_tf = self.get_tableframe('indicators')
        indicators = indicators_tf.filter(topics=('basic',) + topics)

        extractor = UbcovExtractor(universal, merges,
                                   topic_config, value_labels,
                                   column_labels, vars,
                                   indicators, gbd_subnat_map,
                                   topic_value_maps)

        return extractor

    @contextmanager
    def wrap_extractor_with_context(self, extractor,
            extractor_globals={}):
        """
        wraps the extractor (get_extractor) in a context manager which
        automatically cleans up extraction context after the extraction is
        complete.

        Args:
            extractor: An UbcovExtractor instance (usually created from
                       get_extractor().
            extractor_globals: A dictionary used to set the global

        An extraction context can be used to change global defaults, like
        strict, before an extraction is used to change the behavior of the
        extractor. An example of usage is:

        # First, create a config_loader ... then
        extractor = config_loader.get_extractor(...)
        extractor_globals = {"strict": True}
        with config_loader.wrap_extractor_with_context(
                extractor, extractor_globals) as e:
            extraction = e.get_extraction()
            df = extraction.execute()
        """
        if not isinstance(extractor_globals, dict):
            extractor_globals = {"strict": self.strict}
        elif not hasattr(extractor_globals, "strict"):
            extractor_globals["strict"] = self.strict

        extractor_ctx = WinnowerExtractionContext(extractor, extractor_globals)
        extractor_ctx.push()
        yield extractor
        extractor_ctx.pop()

    def initialize(self):
        self._ensure_basic_loaded()
        self._ensure_basic_additional_loaded()
        self._ensure_merges_loaded()
        self._ensure_labels_loaded()
        self._ensure_gbd_subnat_map_loaded()

    def __init__(self, paths, strict=False):
        self.paths = paths
        self.logger = get_class_logger(self)
        self.data_frames = {}
        # If true the match ubCov's behavior as closely as possible even when
        # that behavior is known to be incorrect.
        self.strict = strict

    def get_tableframe(self, name, type=None):
        """
        Return TableFrame for configuration data.

        Args:
            name: name of data set. If specifying a `type`, this will be the
                name of the extraction topic (e.g., 'lri').
            type (default: None): for topic-specific data, the kind of data.
                This must be 'codebooks'.
        Raises:
            Error: an invaid `type` is provided.
            NotFound: No data set with the given name/type could be found.

        This method can be called in one of two forms.

        General data (not topic-specific): Provide just `name`, which should be
        the name of the dataset e.g., 'universal', 'vars', 'merges'. In this
        case do not supply a `type` argument.

        Topic-specific data: provide the topic as `name` and the type of data
        as `type`.
        """
        if type is not None and type not in {'codebooks'}:
            msg = f"{type!r} is not valid - use 'codebooks'"
            raise errors.Error(msg)

        if type and name not in {'design', 'demographics', 'geography'}:
            lookup = self.paths[type]
        else:
            lookup = self.paths

        # the 'basic' sheet includes all 4 topics
        if name in {'universal', 'design', 'demographics', 'geography'}:
            sheet_name = 'basic'
        else:
            sheet_name = name
        try:
            path_or_url = lookup[sheet_name]
        except KeyError:
            if type is None:
                msg = f"No data sheet named {name!r}"
            else:
                msg = f"No {type} sheet named {name!r}"
            raise errors.NotFound(msg)
        df = self._get_dataframe(name, path_or_url)
        if type is None:  # setup
            factory = self.setup_form_classes[name]
            # subset df to only fields in setup forms
            fields = [X.name for X in attr.fields(factory)]
            df = df[fields]
        elif type == 'codebooks':
            ind_df = self.get_tableframe('indicators')
            indicators = ind_df.filter(topics=[name])
            # TODO: Fix breaking on topics where users add additional notes
            # columns that contain specific characers (spaces, parens).
            # They either have to stop doing this or this code needs to drop
            # these columns before calling the get_form function.
            factory = get_form(indicators, all_fields=df.columns)
        else:
            msg = f"No logic to load {name!r} of type {type!r}"
            raise NotImplementedError(msg)
        tf = TableFrame.from_data_and_factory(df, factory,
                                              sheet_name=sheet_name)
        return tf

    def _get_dataframe(self, name, path_or_url):
        if name in self.data_frames:
            return self.data_frames[name]

        # special cases - Demographics, Design, and Geography are stored in
        # same file as Universal config ("basic")
        self.logger.debug(f"Getting dataframe for {name}")
        if name in {'universal', 'design', 'demographics', 'geography'}:
            cols = self._vars_for_topic(name)
            cols.extend(constants.UBCOV_KEY)  # ubcov_id is never present.
            # universal will already have the survey keys. Add them to the
            # others if they don't. A subset test would likely work here as
            # only universal will have the survey keys but this is to future
            # proof the code in case basic is modified in some way.
            for key in constants.SURVEY_KEY:
                if key in cols:
                    continue
                cols.append(key)
            # basic should already have a generated winnower_id so no need to
            # regenerate it.
            cols.append('winnower_id')
            self.logger.debug(f"Copying dataframe from basic using {cols}")
            # do not share the "basic" dataframe - it will be mutated
            df = self.data_frames['basic'][cols].copy()
        else:
            # TODO: try/catch for error (HTTP fail)
            try:
                df = self._read_csv(path_or_url)
            except errors.MultipleValues as e:
                _, col_details = e.args
                prefix = ("Duplicated column names detected!\n\n"
                       "The following column names appear in these columns:")
                details = "\n\t".join(f"{value}: {' '.join(cols)}"
                                      for value, cols in col_details)
                raise errors.MultipleValues(f"{prefix}\n\t{details}") from None

            if 'file_path' in df.columns:
                df['file_path'] = df['file_path'].apply(fix_survey_path)

        df = self._generate_winnower_id(df)

        self.data_frames[name] = df
        return df

    def _generate_winnower_id(self, df):
        """
        Return a winnower_id generated from the survey keys.
        """
        # It may already have a winnower_id ... this could be due to the source
        # of the configs i.e. coming from lesserstones or winnower has already
        # generated a winnower_id for this dataframes source i.e. design is in
        # the basic dataframe and will already have a winnower_id. Either way
        # don't re-generate the winnower_id.
        if 'winnower_id' in df.columns:
            self.logger.debug("Dataframe already has winnower_id. "
                              "Not generating")
            return df

        if not set(constants.SURVEY_KEY).issubset(df.columns):
            self.logger.debug("Dataframe does not contain all "
                              f"SURVEY_KEY columns {constants.SURVEY_KEY} "
                              "Not generating.")
            return df

        def winnower_id(row: pandas.Series):
            "Returns a winnower_id for a row in a larger pandas DataFrame."
            # important - does NOT mutate df
            row['file_path'] = strip_root_and_standardize(row.file_path)
            strs = (str(X).strip().lower() for X in row)
            return hashlib.md5("".join(strs).encode("utf-8")).hexdigest()

        df['winnower_id'] = df[constants.SURVEY_KEY].apply(winnower_id,
                                                           axis='columns')
        return df

    def _ensure_basic_loaded(self):
        """
        Special method to load the "basic" codebook, which is always needed.

        This codebook is an amalgamation of universal configuration and three
        codebooks (Demographics, Design, Geography).
        """
        if 'basic' not in self.data_frames:
            df = self._read_csv(self.paths['basic'])
            df['file_path'] = df['file_path'].apply(fix_survey_path)
            df = self._generate_winnower_id(df)
            self.data_frames['basic'] = df
        # TODO: check the various config objects and LOG any ignored rows

    def _ensure_basic_additional_loaded(self):
        """
        Loads "basic_additional" codebook. This is purely to extract file_id
        for the purpose of naming extraction.
        """
        if 'basic_additional' not in self.data_frames:
            df = self._read_csv(self.paths['basic_additional'])
            self.data_frames['basic_additional'] = df

    def _ensure_merges_loaded(self):
        if 'merges' not in self.data_frames:
            df = self._read_csv(self.paths['merges'])
            df['file_path'] = df['file_path'].apply(fix_survey_path)
            df = self._generate_winnower_id(df)
            self.data_frames['merges'] = df

    def _ensure_labels_loaded(self):
        if 'labels' not in self.data_frames:
            df = self._read_csv(self.paths['labels'])
            df['file_path'] = df['file_path'].apply(fix_survey_path)
            self.data_frames['labels'] = df

    def _ensure_gbd_subnat_map_loaded(self):
        if 'gbd_subnat_map' not in self.data_frames:
            df = self._read_csv(self.paths['gbd_subnat_map'])
            self.data_frames['gbd_subnat_map'] = df

    def get_value_maps(self, topics):
        """
        Returns standardized mapping of values in survey files.

        Survey files contain a large variety of languages, codification
        schemas, and local contextual responses. Value mapping provides a
        simple method of converting those values into a standard schema.

        get_value_maps processes a spreadsheet of values into a simple lookup
        dictionary `res` where res[topic][indicator][survey_value] returns the
        standardized value for post-extraction use.

        Args:
            topics: sequence of str topic names.

        Returns dict of value maps for each topic.
        """
        value_maps = {}

        value_map_paths = self.paths['value_maps']

        for topic in topics:
            if topic not in value_map_paths:
                continue
            topic_value_map = defaultdict(dict)

            df = self._read_csv(value_map_paths[topic])
            # remove columns with null value and/or mapped value
            df = df[df.value.notna() & df.category_indicator_name.notna()]
            # match lower case raw values
            raw_values = df.value.str.lower()
            # example value: 'bottled|w_source_drink'
            for index, cat_ind_name in df.category_indicator_name.iteritems():
                try:
                    mapped_value, indicator_name = cat_ind_name.split('|')
                except ValueError:
                    msg = (f"{cat_ind_name} does not match expected format of "
                           "'MappedValue|IndicatorToMap'")
                    self.logger.error(msg)
                    continue
                else:
                    mapped_value = mapped_value.lower()

                raw_value = raw_values[index]
                ind_map = topic_value_map[indicator_name]
                if raw_value in ind_map and ind_map[raw_value] != mapped_value:
                    msg = (f"Duplicate mapped value for {raw_value} found: "
                           f"using {ind_map[raw_value]} instead of "
                           f"{mapped_value}")
                    self.logger.warning(msg)
                else:
                    # return lower case mapped values
                    ind_map[raw_value] = mapped_value

            value_maps[topic] = topic_value_map

        return value_maps

    def _vars_for_topic(self, topic_name):
        """
        Return list of variable names associated with a topic.
        """
        df = self._get_dataframe('vars', self.paths['vars'])
        # renamed "basic" to "universal" to avoid flubs
        assert topic_name != 'basic', "Request 'universal', not 'basic'"
        if topic_name == 'universal':
            topic_name = 'basic'
        return list(df[df['topic_name'] == topic_name]['var_name'])

    def _read_csv(self, path, **kwargs):
        # Google Sheet has #REF! just like regular Excel
        kwargs.setdefault('na_values', '#REF!')

        # Keep all values as str by default
        kwargs.setdefault('dtype', str)

        def convert_ubcov_id(val):
            if val in {'', '#REF!', '#N/A'}:
                return float('NaN')
            return int(val)

        convert_ubcov_id = partial(pandas.to_numeric, errors='coerce')  # noqa

        kwargs.setdefault('converters', {'ubcov_id': convert_ubcov_id})
        try:
            df = pandas.read_csv(path, **kwargs)
        except HTTPError as e:
            p = urlparse(path)
            base = f"Site {p.netloc!r} responded with error {e.code}"
            if e.code == 429:
                msg = (f"{base} - Too Many Requests. Please wait at least 30 "
                       "seconds and try your extract again.")
            else:
                msg = (f"{base}. See https://en.wikipedia.org/wiki/"
                       "List_of_HTTP_status_codes for additional details.")
            raise errors.Error(msg)
        else:
            # validate non-repeated column names by looking for renamed columns
            detector = re.compile(r'(.+)[.]\d+$')  # detect trailing .e.g, '.1'
            holder = defaultdict(list)

            for col_name, excel_name in zip(df, self._yield_excel_cols()):
                m = detector.match(col_name)
                if m is not None:
                    holder[m.group(1)].append(excel_name)

            if holder:
                # expensive operation, but we're about to crash anyway
                for col_name, excel_name in zip(df, self._yield_excel_cols()):
                    if col_name in holder:
                        holder[col_name].insert(0, excel_name)
                details = list(holder.items())
                raise errors.MultipleValues("Duplicated column names", details)
        return df

    def _yield_excel_cols(self):
        """
        Yield excel column names in an infinite stream.
        """
        yield from string.ascii_uppercase

        i = 2
        while True:
            yield from ("".join(letters)
                        for letters in
                        itertools.product(*[string.ascii_uppercase for
                                            _ in range(i)]))
            i += 1

    def _get_label_dicts(self, paths):
        self._ensure_labels_loaded()
        labels_tf = self.get_tableframe('labels')
        label_config = labels_tf.filter(file_paths=paths)
        value_labels = {}
        column_labels = {}

        for label in label_config:
            if label.is_value_label():
                self._handle_value_label(value_labels, label)
            if label.is_column_label():
                self._handle_column_label(column_labels, label)

        return value_labels, column_labels

    def _handle_value_label(self, value_labels, label):
        file_labels = value_labels.setdefault(label.file_path, {})
        labels = file_labels.setdefault(label.variable, {})
        if label.value_num in labels and \
                labels[label.value_num] != label.value_str:
            msg = (f"Duplicate label for file_path {label.file_path} "
                   f"variable {label.variable} value {label.value_num}: "
                   f"{labels[label.value_num]} and {label.value_str}. "
                   f"Using {label.value_str}")
            self.logger.warning(msg)
        labels[label.value_num] = label.value_str

    def _handle_column_label(self, column_labels, label):
        if label.variable in column_labels and \
                column_labels[label.variable] != label.variable_label:
            cur_label = column_labels[label.variable]
            msg = (f"Duplicate label for file_path {label.file_path} "
                   f"variable {label.variable}: {cur_label} "
                   f"and {label.variable_label}. Using {cur_label}")
            self.logger.warning(msg)
        column_labels[label.variable] = label.variable_label


class UbcovExtractor:
    """
    Full configuration for performing an extraction.
    """
    # Indicators which may no longer be in use.
    _ignored_indicator_names = {
        'psu_recode', 'strata_recode',
        # TODO: add universal config
    }

    # vars known to be defined but have no current use
    # TODO: clean these up in ubCov
    _ignored_var_names = {
        'pweight_admin_1', 'pweight_admin_2', 'pweight_admin_3',
    }

    # This class is not yet complete. It performs only basic extractions
    def __init__(self, universal, merges, topic_config, value_labels,
                 column_labels, vars, indicators, gbd_subnat_map,
                 topic_value_maps):
        self.universal = universal
        self.merges = merges
        self.topic_config = topic_config
        self.topics = tuple(topic_config)
        self.value_labels = value_labels  # [filename][column][value] -> label
        self.column_labels = column_labels  # [column] -> label
        self.vars = vars  # tuple of var objects
        self.indicators = tuple(indicators)
        self.created_indicator_names = []  # populated by transform objects
        self.gbd_subnat_map = gbd_subnat_map
        self.topic_value_maps = topic_value_maps

        def get_set(iterable, *, flatten=False):
            if flatten:
                result = set()
                for inputs in filter(bool, iterable):
                    result.update(inputs)
            else:
                result = set(iterable)
            result.discard(None)  # not a valid input (but a common default)
            return result
        self.input_vars = get_set(X.input_vars for X in indicators)
        self.input_meta = get_set((X.input_meta for X in indicators),
                                  flatten=True)
        self.logger = get_class_logger(self)

    def get_extraction(self, keep_unused_columns=False,
                       save_unmapped_values=True):
        # Fail Fast if no extraction context. This is unlikely to occur unless
        # the user has altered the extraction context in some unexpected way.
        # By default the extraction context (and extraction globals) are
        # generated with sensible defaults and the user does not need to worry
        # about them. If for some reason there is no extraction context then
        # an attempt to access the the extraction context globals will raise
        # an exception alerting the user to an unexpected problem with the
        # extraction context.
        if eg.strict:
            self.logger.warning("Extracting in strict mode")

        pre, post_cc, post = get_hooks(self.universal,
                                       self.topic_config.keys(),
                                       self._config_dict())
        have_hook = bool(pre or post_cc or post)

        builder = self.get_source(have_hook=have_hook)
        # Reshape
        # Subset
        self.apply_merges(builder)
        if pre:  # pre_extraction hook
            builder = self.apply_hook(pre, builder)
        self.apply_labels(builder)
        self.apply_vars(builder)
        self.apply_indicators(builder)
        self.apply_value_map(builder, save_unmapped_values)
        builder = self.apply_custom_code(builder)
        if post_cc:  # post_custom hook
            builder = self.apply_hook(post_cc, builder)
        # validation
        self.clean_extraction(builder, keep_unused_columns=keep_unused_columns)
        # map gbd subnat + india_urban
        self.apply_gbd_subnat_map(builder)
        if post:  # post_extraction hook
            builder = self.apply_hook(post, builder)
        return builder.get_chain()

    @log_notimplemented
    def get_source(self, *, have_hook=False):
        univ = self.universal
        self.logger.info(f"Input file path: {univ.file_path}")
        # delimiter indicates a CSV file is to be loaded
        path = resolve_path_case(univ.file_path)
        source = FileSource(path, delimiter=univ.delimiter)
        chain = ValidatedExtractionChain(source)
        # wrap chain into ExtractionChainBuilder which validates
        # that efficient column loading can continue and handles
        # merges and their right_chain in particular in the process.
        builder = ExtractionChainBuilder(chain)

        # Logic for extracting Stata value labels
        # TODO: this needs to happen on every merge as well!
        #   cache _columns_informing_categoricals (?)
        #   Refactor this into a method taking `chain` as sole arg
        MapNumericValues.if_present(self._columns_informing_categoricals(),
                                    builder.source.get_value_labels(),
                                    builder)

        if univ.reshape:
            # TODO: common problem of running EVERY value through this...
            reshape_cols = [chain.get_input_column(X) for X in univ.reshape]
            builder.append(Reshape(reshape_cols,
                                   # note kwarg changes name
                                   under_delim=univ.reshape_stem,
                                   keepid=univ.reshape_keepid))

        if univ.subset:
            if not isinstance(univ.subset, str):
                msg = f"subset configuration {univ.subset!r} is not a str"
                self.logger.error(msg)
            else:
                self.logger.debug(f"univ.subset is {univ.subset}")
                self._apply_subset(builder, univ.subset, have_hook=have_hook)
        return builder

    def _apply_subset(self, builder, subset, *, have_hook):
        # subset = subset.strip()  # TODO: form handling should clean this
        if subset.startswith('gen'):
            try:
                transform = self._handle_gen(gen_stmt=subset)
            except errors.InvalidExpression:
                Subset.log_bad_query(query=subset, have_hook=have_hook,
                                     logger=self.logger)
                return
        else:
            # All columns cannot be collected from Subset transform
            # and it invalidates efficient column loading.
            msg = ("Cannot efficiently load columns when you have"
                   "Subset queries. Reading the whole dataframe.")
            self.logger.warning(msg)
            transform = Subset(query=subset, have_hook=have_hook)
        builder.append(transform)

    def _handle_gen(self, gen_stmt):
        """
        Handle backwards compatibility for subset column.

        The original ubcov code did the equivalent of eval() on the contents
        of the subset column. For most cases this was used for subset commands.

        For other cases a myriad of things were done, including executing
        arbitrary scripts present elsewhere in the file system.

        A very common behavior was to generate a new column in the data set.
        Usually this was used to add information that was implicit to the data
        itself, such as:
            the observation weight (when all weights are equal)
            the sub-national administrative unit (when all rows are from the
                same place)
            the interview month (when all interviews were conducted in August)

        This method is meant to cover the case of generating a column to a
        single value, either integer or string.
        """
        pattern = r'^gen (\w+)\s*=\s*(.+)$'
        match = re.match(pattern, gen_stmt)
        if match is None:
            msg = f"Unable to interpret subset generate statement {gen_stmt!r}"
            raise errors.InvalidExpression(msg)

        # When generating a column as a string the value is QUOTED, so check
        # for quotes. Stata only uses double quotes for strings
        column, val = match.groups()
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        else:
            try:
                val = int(val)
            except ValueError:
                msg = (f"Invalid number in generate statement: {val!r}. "
                       "String values must be double-quoted")
                raise errors.InvalidExpression(msg)
        return ConstantColumn(column=column, value=val)

    def _columns_informing_categoricals(self):
        """
        Returns the set of columns which inform categorical indicators.

        These columns may have embedded value maps with their source data file,
        which needs to be manually requested.
        """
        # Determine categorical indicators we will produce.
        categorical_vars = {X.input_vars for X in self.indicators
                            # TODO: magic str
                            if is_CategoricalStr_indicator(X,
                                                           self.topic_config[X.topic_name]
                                                           # There exists a weird edge case where
                                                           # "basic" indicators do not have a
                                                           # configuration, input None instead
                                                           if X.topic_name in self.topic_config
                                                           else None)}

        # Determine the column(s) providing their data.
        columns = set()
        for topic_config in self.topic_config.values():
            for var_name, configured_cols in attr.asdict(topic_config).items():
                if not configured_cols:
                    continue
                if var_name in categorical_vars:
                    # configured_cols may be a list of cols or a single col
                    if isinstance(configured_cols, list):
                        columns.update(configured_cols)
                    elif isinstance(configured_cols, str):
                        columns.add(configured_cols)
                    else:
                        msg = (f"Expected configured_cols {configured_cols!r} "
                               "to be a str or a list, but it is neither.")
                        raise RuntimeError(msg)
        return columns

    def apply_merges(self, builder):
        """
        Apply merge transforms to component, returning a new component.
        """
        columns_to_label = self._columns_informing_categoricals()
        for merge_config in self.merges:
            merge = Merge.from_chain_and_config(
                builder, merge_config, columns_to_label=columns_to_label)
            builder.append(merge)
            self.apply_labels(builder, filename=merge_config.merge_file)

    def apply_hook(self, hook, builder):
        """
        If each method in pre/post-cc/post-extraction does not advertise
        which extra column/s it uses, ValidatedExtractionChain cannot be
        guaranteed, so fallback to ExtractionChain.

        Args:
            hook: Pre-extraction/Post-custom/Post-Extraction hook transforms.
            chain: Possibly ValidatedExtractionChain, definitely ExtractionChain # noqa
        """
        builder.append(hook)
        return builder

    def apply_labels(self, builder, *, filename=None):
        """
        Apply label transforms, returning a new component.
        """
        filename = filename or self.universal.file_path
        labels = self.value_labels.get(filename)
        if labels:
            for var, value_mapping in labels.items():
                self._value_map_column(builder, var, value_mapping.items())

    def apply_gbd_subnat_map(self, builder):
        """
        Apply gbd subnational map transform, return new component.
        """
        # create an object with the subnat info for the particular survey
        # Note: as this is called *after* clean_extraction there is no
        #       bookkeeping regarding created_indicator_names
        #       MapSubnationals will populate admin_1_id
        #       otherwise ihme_loc_id must be an India subnational.
        if self.gbd_subnat_map is not None:
            builder.append(MapSubnationals(self.gbd_subnat_map))

        if self.universal.ihme_loc_id.startswith('IND'):
            builder.append(MapIndiaUrbanRuralSubnationals())

    def apply_value_map(self, builder, save_unmapped_values):
        """
        Categorize strings by applying value map transform.

        Mutates self.created_indicator_names to include all mapped indicators.
        """
        if not self.topic_value_maps:
            return

        mappable_indicators = [X for X in self.indicators
                               if X.map_indicator]
        mv = MapValues(self.topic_value_maps, mappable_indicators,
                       write_unmapped_to_tempdir=save_unmapped_values)

        mapped_indicators = mv.mapped_indicators(self.created_indicator_names)
        self.created_indicator_names.extend(mapped_indicators)

        builder.append(mv)

    def _value_map_column(self, builder, column, label_pairs):
        """
        Example method doing what is necessary.

        This should likely become a private method of UbcovExtractor
        """
        try:
            name = builder.get_input_column(column)
        except errors.ValidationError:
            msg = f"Expected to find {column!r} in data but did not."
            self.logger.error(msg)
            return
        else:
            self.logger.info(f"Applying value maps for {column} {label_pairs}")
            builder.append(MapNumericValues(name, label_pairs))

    def _var_map_column(self, builder, column, label_pairs):
        """
        Takes chain object and match (label_pair) for
        a var that needs a label to be added (from
        labels db).

        Overwrites original _var_labels in source with user-
        entered labels in labels db.
        """
        # check and see if self.source refers to different files
        # problem: chain.source._var_labels refers to different
        #   source objects with each iteration (from merges)
        # this method for categ_multi_bin requires that they
        # all append to the same object
        # chain.source._var_labels are the original labels in the dataset
        try:
            name = builder.get_input_column(column)
        except errors.ValidationError:
            msg = f"Expected to find {column!r} in data but did not."
            self.logger.error(msg)
            return
        else:
            self.logger.info(f"Applying var maps for {column} {label_pairs}")
            # if _label_vars is empty you have to make it a dict first
            # otherwise you get a "cant with None type"
            if builder.source._var_labels is None:
                builder.source._var_labels = {}
            builder.source._var_labels[name] = dict(label_pairs)[column]

    def apply_vars(self, builder):
        """
        Applys Input and Meta Vars in preparation for computing Indicators.

        Indicators are computed from Input Vars and Meta Vars. An Input Var
        refers to a column in the raw input survey that can be transformed into
        an output column, possibly with the assistance of some information
        about the contents of the column.

        Meta Vars are the information about the column.

        Example: the "urban" indicator is a binary value where 1 indicates the
        geographic location is considered an urban area.

        "urban" is a Input Var which references the column containing this
        information. "urban_true" and "urban_false" are Meta Vars which each
        contain the list of values in "urban" which correspond to "this is an
        urban area" and "this is not an urban area", respectively.
        """
        # holds a mapping of (original_input_column: renamed_input_column)
        # these renames are necessary when an input file has a column whose
        # name matches an ubCov `var`. First encountered example: "strata"
        renames = {}

        for var in self.vars:
            if var.var_name in self.input_vars:
                self._apply_input_var(renames, builder, var)
            elif var.var_name in self._ignored_var_names:
                msg = f"{var.var_name} is neither input_var nor input meta!"
                self.logger.error(msg)

    def _rename_column_if_necessary(self, rename_map, builder, col):
        """
        Rename `col` and update `rename_map` IFF col already exists.

        Mutates chain and rename_map.

        Args:
            rename_map (dict): (original_name: new_name) mappings.
            chain: the ExtractionChain being operated on.
            col (str): name of the column.

        Returns the renamed column or None if no rename occurred.
        """
        if col in builder.output_columns():  # TODO: O(n) check
            renamed_col = f"{col}_orig"  # TODO: magic string
            builder.append(RenameColumn(col, renamed_col))
            rename_map[col] = renamed_col  # TODO: check for conflict?
            return renamed_col

    def get_indicator(self, vname):
        """
        Return the indicator given a var input.
        """
        # match vname to either input_meta or input_vars
        for indic in self.indicators:
            # input_meta is always a tuple
            if vname in indic.input_meta:
                return indic
            # input_vars can be a single string or an iterable
            if isinstance(indic.input_vars, str):
                if vname == indic.input_vars:
                    return indic
            if indic.input_vars is not None:
                if vname in indic.input_vars:
                    return indic

    def _apply_input_var(self, rename_map, builder, var):
        """
        Aliases a column in our extraction chain to name of the Input Var.
        """
        vname = var.var_name

        if vname == 'smaller_site_unit':
            ssu = self._get_topic_config_value(var.topic_name, vname)
            # one-off issue. smaller_site_unit is meant to be copied to the
            # data set as a literal value (consistent for all rows)
            # NOTE: `source_name` is actually the configured value (1 or 0)
            builder.append(ConstantColumn(vname, ssu))
            return

    def apply_indicators(self, builder):
        """
        Add indication generation to our transformation operations.
        """
        label_helper = LabelHelper(builder, self.column_labels)

        config = self.get_indicator_config()

        var_names = [v.var_name for v in self.vars]
        self.logger.debug(f"Applying indicators, vars are {var_names}")
        for ind in self.indicators:
            self.logger.debug(f"Applying indicator: {ind}")
            # Check to see that the indicator input_var is actually defined in
            # vars. This prevents the construction of eroneous indicators.
            if (isinstance(ind.input_vars, str)
                    and ind.input_vars not in var_names  # noqa
                    and not ind.code_custom):  # noqa

                msg = f"Skipping unknown input_var for {ind.indicator_name}"
                self.logger.info(msg)
                continue

            if ind.indicator_name in self._ignored_indicator_names:
                msg = f"Skipping ignored indicator: {ind.indicator_name}"
                self.logger.info(msg)
                continue

            # If strict maybe rename input column ... also logs some warnings
            # and useful debug info.
            self._strict_maybe_rename_input_col(builder, config, ind)

            if ind.code_custom:
                # assume indicator is created and record as such
                self.created_indicator_names.append(ind.indicator_name)
                msg = f"Skipping code_custom indicator {ind.indicator_name}"
                self.logger.debug(msg)
                continue  # custom code module will make this work

            transform = transform_from_indicator(ind, config, label_helper)

            if transform is not None:
                builder.append(transform)
                self.created_indicator_names.append(ind.indicator_name)
            else:
                if ind.indicator_required:
                    msg = (f"Indicator {ind.indicator_name!r}: "
                           "missing required columns")
                    raise errors.RequiredInputsMissing(msg)
                # TODO: can we provide more information?
                # This can be because ind.indicator_type isn't supported
                # OR
                # because ind.input_vars aren't populated (there are none)
                msg = f"Skipping indicator {ind.indicator_name!r}"
                self.logger.info(msg)

    def _strict_maybe_rename_input_col(self, builder, config, ind):

        # Sometimes a dataset will include the indicator column already. In
        # these cases the configuration for the indicator can be empty.
        # Because ubCov lowercases all input columns at the time the data
        # is loaded the input column will have the same name as the
        # indicator and nothing else is required. However, winnower does
        # not lowercase column names and so an additional step is needed to
        # ensure that input columns that match indicator names, ignoring
        # case, will be correctly renamed to the lowercased indicator name.
        #
        # 1. Check to see if the indicator is defined in the config but has
        #    no value.
        #
        # 2. If true then search the input columns for the indicator
        #    ignoring case.
        #
        # 3. If an input column is found and the input column only matches
        #    the indicator name when case is ignored then append a Rename
        #    Transform to the extraction chain.
        #
        # NOTES: This is performed before the custom_code check because
        # there are situations were custom code will expect the renamed
        # indicator to be present.

        ind_name = ind.indicator_name
        if hasattr(config, ind_name) and not getattr(config, ind_name):
            # The config has this indicator but it's input is undefined.
            # Check to see if there is already a column with a similar name
            # in the input data.
            msg = (f"Indicator {ind_name} is present in the topic config "
                   "but is undefined. Checking for input column with "
                   "similar name.")
            self.logger.info(msg)
            try:
                input_name = builder.get_input_column(ind_name)
            except errors.ValidationError:
                d_msg = (f"Indicator {ind_name} is not present in "
                         "input columns, renaming unecessary.")
                self.logger.debug(d_msg)
            else:
                if input_name != ind_name:
                    if eg.strict:
                        msg = ("STRICT: Renaming indicator input column "
                               f"{input_name} to {ind_name}.")
                        self.logger.warning(msg)
                        builder.append(RenameColumn(input_name, ind_name))
                    else:
                        msg = (f"Indicator {ind_name} is undefined but "
                               f"input column {input_name} matches when case "
                               "is ignored. Please configure this indicator!")
                        self.logger.warning(msg)

                else:
                    msg = (f"Indicator {ind_name} is identical to input "
                           f"column: {input_name}, but indicator is not "
                           "configured. This may cause problems!")
                    self.logger.warning(msg)

    def apply_custom_code(self, builder):
        for topic in self.topics:
            try:
                module = importlib.import_module(f"winnower.custom.{topic}")
            except ModuleNotFoundError as e:
                if self._custom_code_exists_for_ubcov(topic):
                    msg = f"Error importing custom code for {topic}: {e}"
                    self.logger.error(msg)
                else:
                    msg = (f"No custom code found for {topic}. "
                           "This is probably OK")
                    self.logger.info(msg)
            else:
                if hasattr(module, 'Transform'):
                    cfg = self._config_dict()
                    code_transform = module.Transform(
                        builder.output_columns(),
                        cfg,
                        ExtractionMetadata.from_UbcovExtractor(self),
                    )
                    builder.append(code_transform)
                    self.logger.info(f"Added custom code for {topic}")
                else:
                    msg = ("Could not find 'Transform' class in custom code "
                           f"for {topic}")
                    self.logger.error(msg)
        return builder

    def _custom_code_exists_for_ubcov(self, topic):
        "Predicate: does custom code exist in ubCov for this topic?"
        special_cases = {
            'sti_symptoms',  # file exists, but applies to 1 survey. 2019-01-30
        }
        if topic in special_cases:
            return False

        p = Path('<FILEPATH>'
                 f'topics/code/{topic}.do')
        return p.exists()

    def _config_dict(self):
        """
        Returns configuration in a flat dict.

        Performs a safe merge of values, preventing accidental overwriting.

        Raises MultipleValues if more than 1 non-None value is provided.
        """
        # TODO: cache?
        # fields in this are known to be default and thus, of less importance
        # than any non-default value
        fields_with_null_input = set()
        result = {}
        for config in self.topic_config.values():
            for field, val, val_is_default in attr_items(config):
                if field in fields_with_null_input:
                    result[field] = val  # Note: last default value wins
                    if not val_is_default:
                        fields_with_null_input.remove(field)
                elif field not in result:
                    result[field] = val
                    if val_is_default:
                        fields_with_null_input.add(field)
                elif val_is_default:
                    continue  # keep existing non-default value
                else:
                    cur = result[field]
                    if cur != val:
                        msg = f"Multiple config values for {field}"
                        raise errors.MultipleValues(msg, field, (cur, val))
        # update with universal config last, as some names are copied between
        # configs but universal is the only one guaranteed parsed correctly
        for name, value, _ in attr_items(self.universal):
            result[name] = value
        return result

    def clean_extraction(self, builder, *, keep_unused_columns):
        """
        Clean extranneous data
        """
        indicators = list(self.created_indicator_names)
        # assume all custom code indicators are created; if they're all NaN we
        # clean them in a subsequent step)
        indicators.extend(X.indicator_name for X in self.indicators
                          if X.code_custom)
        #   remove non-indicator columns (below - refactor into method)
        if not keep_unused_columns:
            self.remove_non_indicators(builder,
                                       keep_cols=self.created_indicator_names)
        #   order output columns
        #   remove rows that are missing all non-meta indicators

        builder.append(DropEmptyRows(self.indicators))

    def remove_non_indicators(self, builder, keep_cols):
        """
        Add commands to remove all columns not defined as indicator outputs.

        This is a data cleaning operation - removing unnecessary columns.
        """
        # TODO
        # This should be changed to iterate through the chain and keep tabs of
        # all the input columns even being used
        #
        # This requires a major refactor of all Transform components + Custom
        # code to advertise externally what values they REQUIRE.
        #
        # It may or may not be prudent to then provide a default _validate
        # method to winnower.transform.base.Component since, in theory,
        # most _validate calls should have the same logic
        builder.append(KeepColumns(keep_cols, drop_empty=True))

    # configuration lookup relies on these
    def file_paths(self):
        # self.universal.file_path and merge_file for all merges
        # self.universal.file_path is always first!
        raise NotImplementedError("Return seq of paths")

    def id(self):
        """
        Return id value, used to look up other values.
        """
        return self.universal.ubcov_id

    def _get_topic_config_value(self, topic, name):
        """
        Gets the configuration value named `name` for `topic`.

        Raises an AssertionError if configuration contains multiple values.
        """
        values = self._get_topic_config_values(topic, name)
        if not values:
            return
        if isinstance(values, tuple):
            if len(values) > 1:
                msg = f"Multiple values for topic {topic}.{name} - {values}"
                raise errors.MultipleValues(msg, values)
            else:
                return values[0]
        return values

    def _get_topic_config_values(self, topic, name):
        """
        Gets the configuration values named `name` for `topic`.
        """
        try:
            return getattr(self.topic_config[topic], name)
        except KeyError:
            msg = f"No topic config for {topic!r}"
            tb = sys.exc_info()[2]
            raise KeyError(msg).with_traceback(tb) from None
        except AttributeError:
            msg = f"No config {name!r} for topic {topic!r}"
            tb = sys.exc_info()[2]
            raise NotImplementedError(msg).with_traceback(tb) from None

    def get_indicator_config(self):
        """
        Returns configuration pertinent to indicator instance.
        """
        # Surprising behavior - topics sometimes share configuration
        #
        # example: vaccination topic defines the birth_year and birth_month
        # indicators but these are in fact columns created by age_calculator
        # as part of the demographics custom code.
        #
        # Simplest solution is to share all configuration. Due to historical
        # code expectations these are exposed using attributes instead of as a
        # dict
        return Namespace(**self._config_dict())


class TableFrame:
    """
    Encapsulates a DataFrame, providing more restricted interface to get data.

    The DataFrame-as-table is not an entirely correct analogy, especially
    regarding expectations about data consistency one would expect.

    TableFrame is used as a facade to support eventual replacement of the
    CSV-based backend with a relational database.

    Instantiate with factory method TableFrame.from_df OR be sure to call
    table_frame.prune_invalid_rows() before any usage.
    """
    @classmethod
    def from_data_and_factory(cls, df: pandas.DataFrame, factory, sheet_name):
        logger = get_class_logger(cls)
        result = cls(logger, factory, df, sheet_name)
        required = required_fields(factory)
        result.prune_invalid_rows(required)
        return result

    def __init__(self, logger, result_factory, df, sheet_name):
        """
        Args:
            logger: logging.Logger-compatible instance for log messages.
            non_null_columns: collection of str column names which must be
                present for a row in the dataframe to be valid.
            result_factory: factory method used to produce result values.
            df: DataFrame containing source data.
            sheet_name: name of spreadsheet table loaded from.
        """
        self.logger = logger
        self.result_factory = result_factory
        self.df = df
        self.sheet_name = sheet_name
        # These are used by get() method to provide a better error message
        self.deleted_ubcov_ids = []
        self.deleted_winnower_ids = []
        self.required_columns = []

    def prune_invalid_rows(self, non_null_columns):
        """
        Remove rows from data frame that do not contain all required columns.

        Returns removed data.

        This method should be called before any get() or filter() calls to
        ensure only valid data is returned.
        """
        if not non_null_columns:
            return

        has_null_value_in_row = rows_with_nulls_mask(non_null_columns, self.df)

        removed = self.df[has_null_value_in_row]
        self.logger.debug("Removing {} rows with NULLs in column(s) {}".format(
            len(has_null_value_in_row), non_null_columns))

        # Collecting ubcov_ids for removed rows for better error-message
        self.required_columns = non_null_columns
        if 'ubcov_id' in non_null_columns:
            removed_uids = self.df['ubcov_id']
            removed_uids = removed_uids[has_null_value_in_row]
            removed_uids = downcast_float_to_integer(removed_uids.dropna())
            self.deleted_ubcov_ids = list(removed_uids)

        # Collecting winnower_ids for removed rows for better error-message
        if 'winnower_id' in non_null_columns:
            removed_wids = self.df['winnower_id']
            removed_wids = removed_wids[has_null_value_in_row]
            self.deleted_winnower_ids = list(removed_wids)

        self.df = self.df[~has_null_value_in_row]
        return removed

    # ORM-like query methods.
    # Arguments intended to resemble Django's get()/filter() and/or
    # SQLAlchemy's get()/filter_by() methods.
    def get(self, *, ubcov_id=None, winnower_id=None):
        """
        Get single record.  Provide only one of ubcov_id/winnower_id.

        Keyword Args:
            ubcov_id: id value to return a record for. Must be keyword arg.
            winnower_id: winnower id for a record. Must be a keyword arg.

        Raises:
            NotFound: no record exists with that id OR multiple results exist.
            Error: `ubcov_id` not a valid field in this TableFrame.

        Returns instance of self.result_factory.
        """
        if ubcov_id and winnower_id:
            raise errors.Error("Provide only one of ubcov_id/winnower_id")

        if ubcov_id is not None:
            key = "ubcov_id"
            value = ubcov_id
        else:  # winnower_id
            key = "winnower_id"
            value = winnower_id

        try:
            mask = self.df[key] == value
        except KeyError:
            err = "{} not valid for this TableFrame (cols: {})"
            raise errors.Error(err.format(key, self.df.columns))

        candidates = self.df[mask]

        if len(candidates) == 1:
            return self._factory_values(candidates)[0]
        else:
            if len(candidates):
                msg = f"Multiple rows with {key} {value}"
            else:
                if (key == 'ubcov_id' and value in self.deleted_ubcov_ids) \
                        or (key == "winnower_id" and value in self.deleted_winnower_ids):  # noqa
                    msg = (f"Row with {key} {value} removed for missing "
                           f"required column/s: {self.required_columns}")
                else:
                    msg = f"No row with {key} {value}"
            raise errors.NotFound(f"{msg} in {self.sheet_name} sheet")

    def filter(self, *, file_paths=None, topics=None, winnower_ids=None):
        """
        Return record(s) matched by either file_path, topics or winnower_ids.

        Keyword Args:
            file_paths (str): collection of file paths to filter against. May
                also pass a Path instance.
            topics: collection of str topic_name values. Filters any record
                with a topic_name in topics.
            winnower_ids: collection of str winnower_id values. Filters any
                record with a winnower_id in winnower_ids.

        Raises:
            Error: if both or neither of (file_path, topics) are provided.

        Returns list of objects matching filter criteria. Objects will be
        produced by self.result_factory
        """
        if file_paths is None and topics is None:
            raise errors.Error("Must provide either file_path or topics")

        res_df = self.df
        if file_paths is not None:
            # convert Path objects if necessary
            file_paths = [str(path) for path in file_paths]
            res_df = res_df[res_df['file_path'].isin(file_paths)]
        if topics is not None:
            res_df = res_df[res_df['topic_name'].isin(topics)]

        # Filter on winnower_id if present in the data frame.
        # Winnower_id is a hash of a concatenation of the IHME survey keys. See
        # constants.SURVEY_KEY and generate_winnower_id for details.
        if winnower_ids and 'winnower_id' in res_df.columns:
            self.logger.debug(f"Filtering on winnower_ids {winnower_ids}.")
            res_df = res_df[res_df['winnower_id'].isin(winnower_ids)]
            self.logger.debug(f"Filtered result (ubcov_id): {res_df.ubcov_id}")

        return self._factory_values(res_df)

    def __iter__(self):
        for i in range(self.df.shape[0]):
            try:
                yield self.result_factory(**self.df.iloc[i].to_dict())
            # https://www.python.org/dev/peps/pep-0342/#new-generator-method-close
            except GeneratorExit:
                return
            except:  # noqa blanket catch for errors from result_factory
                pass

    # Helper method
    def _factory_values(self, df):
        """
        Return list of result_factory instances built from dataframe.
        """
        return [self.result_factory(**df.iloc[i].to_dict())
                for i in range(df.shape[0])]


class LabelHelper:
    """
    Handles retrieving and consolidating column labels.

    Labels may be defined or modified in the following ways

    - a winnower.sources.FileSource may include labels
    - a winnower.transform.Reshape may modify labels
    - a winnower.transform.Merge may add additional labels from the merged file
    - an external system may provide labels as case_insensitive_column_labels

    This class is only partially implemented, handling the common cases.
    """
    def __init__(self, builder, case_insensitive_column_labels):
        self.builder = builder
        self.case_insensitive_column_labels = case_insensitive_column_labels
        self.logger = get_class_logger(self)

    def get_column_labels(self):
        # 1) chain.source has get_column_labels()
        # 2) Reshape() transforms may alter these
        # 3) Merge() transforms may ADD labels (NO API YET)
        # 4) ubcov_extractor.column_labels are provided by user
        #
        # The above are prioritized low-to-high (4 == high).
        source_labels = self.builder.source.get_column_labels()
        # TODO: get Reshape instances from source_chain
        #       reshape_inst.column_renames can be used to TRANSLATE columns
        #       for renamed, orig_cols in reshape_inst.var_maps.items():
        #           if renamed not in labels:
        #               source_cols = iter(orig_cols)
        #               for col in source_cols:
        #                   if col in labels:
        #                       labels[renamed] = labels.pop(col)
        #                       break
        #               # clean up remaining labels
        #               for col in source_cols:
        #                   if col in labels:
        #                       del labels[col]
        #
        # TODO: test stata behavior to ensure the above matches
        reshape_labels = {}
        self.logger.critical("WARNING: ignoring labels on reshaped columns")
        # Get Merge instances from source_chain.
        #       merge_inst.column_labels has the data
        #       2 cases:
        #           1: column was NOT renamed - will be present in df
        #           2: column WAS     renamed MUST TEST STATA BEHAVIOR
        #              (my suspicion is ONLY keep label IFF no label exists)
        #              Warns user if this happens but keep all labels for now.
        merge_labels = {}

        # ExtractionChainBuilder does not have iter() implemented, so get
        # chain (ExtractionChain or ValidatedExtractionChain) before iterating.
        if isinstance(self.builder, ExtractionChainBuilder):
            chain = self.builder.get_chain()
        else:
            chain = self.builder

        merged_labels = [X.column_labels for X
                         in get_merges_from_chain(chain)]

        renamed_columns = []
        # check for renamed columns within merged_labels (list of dict items)
        for labels in merged_labels:
            renamed_columns = [k for k in merge_labels
                               if k in labels
                               and merge_labels[k] != labels[k]]
            merge_labels.update(labels)

        # check for renamed columns in merge_labels against source labels
        renamed_columns.extend(k for k in source_labels
                               if k in merge_labels
                               and source_labels[k] != merge_labels[k])

        if renamed_columns:
            columns_renamed = ", ".join(renamed_columns)
            self.logger.critical(
                f"The following column(s) had conflicting labels: "
                f"be sure to check your outputs: {columns_renamed}")

        db_labels = self.case_insensitive_column_labels

        labels = {}

        def _update_and_log_overwrite(new_labels):
            present = [k for k in new_labels if k in labels]
            if present:
                updated_labels = ", ".join(present)
                self.logger.error(f"Error: These column(s) were overwritten: "
                                  f"{updated_labels}")
            labels.update(new_labels)

        _update_and_log_overwrite(source_labels)
        _update_and_log_overwrite(reshape_labels)
        _update_and_log_overwrite(merge_labels)
        # configuration ignores case so we must manually resolve it
        _update_and_log_overwrite({self.builder.get_input_column(key): value
                                   for key, value in db_labels.items()})

        return labels

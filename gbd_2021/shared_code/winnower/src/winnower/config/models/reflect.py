"""
Reflects configuration processing classes based off of Indicator configuration.
"""
import logging


import attr
import re
from keyword import iskeyword

from winnower.errors import Error, MultipleValues

from . import fields
from .indicators import fields_for_indicator
from .topic_forms import TopicFormManager


# Default fields used for reflection
# this has heavy overlap with setup_forms.UniversalForm
DEFAULT_FIELDS = {
    'nid': fields.value(required=True, converter=int),
    'year_start': fields.value(required=True, converter=int),
    'year_end': fields.value(required=True, converter=int),
    'file_path': fields.required_shared_filesystem_path(),
}

CONFLICTING_FIELD_PROCESSOR_MSG = (
    "Conflicting input processing for field {}! Please contact the winnower "
    "team to ensure your data is being correctly processed"
)


logger = logging.getLogger(__loader__.name)


def get_form(indicators, all_fields):
    """
    Returns a form class for processing indicator configuration.

    Args:
        indicators: sequence of IndicatorForm instances, specifying what will
                    be processed.
        all_fields: sequence of string field names enumerating all possible
                    fields to be processed.

    Raises: Error if `indicators` is empty, or MultipleValues if `indicators`
        come from multiple topics.
    """
    if not indicators:
        raise Error("No indicators provided")

    if all_fields is None:
        all_fields = []

    topics = {X.topic_name for X in indicators}

    if len(topics) != 1:
        raise MultipleValues("Multiple topics provided")
    topic, = topics

    base = TopicFormManager.for_topic(topic)
    # pull in entries from indicators db
    field_processors = processors_fields_for_indicators(indicators)
    # get_form will generate all fields implied by the list of
    # IndicatorForm instances provided. In addition it supports a catch-all
    # all_fields which forces additional unhandled fields.
    #
    # the use case for this method was to create forms to handle rows of data
    # provided by CSV documents, and the all_fields portion was necessary to
    # handle fields that had nothing to do with indicator processing and were
    # human-centric. This allows users to add arbitrary columns to their
    # codebooks without breaking things
    extra_fields = (X for X in all_fields if X not in field_processors)
    field_processors.update(default_fields_for_form(extra_fields))
    form_cls = create_topic_config_form(topic, base, field_processors)
    return form_cls


def processors_fields_for_indicators(indicators) -> dict:
    "Returns dict of {field_name: attribute_processor} for indicators' fields."
    result = {'ubcov_id': fields.ubcov_id_field()}
    for ind in indicators:
        fp = fields_for_indicator(ind)  # field processors
        for field in fp:
            if field in result:
                logger.warning(CONFLICTING_FIELD_PROCESSOR_MSG.format(field))
        result.update(fp)
    return result


def default_fields_for_form(field_names):
    """
    Returns a dict of {field_name: attribute_processor} for field_names.
    """
    return {
        f: DEFAULT_FIELDS[f] if f in DEFAULT_FIELDS else fields.value()
        for f in field_names}


def create_topic_config_form(topic, base, field_processors):
    """
    Returns class for parsing form inputs.

    This returns a class that is *very similar* to a subclass of `base`, except
    that it is not a subclass of base. This is because atts.make_class does not
    currently handle using base classes with optional arguments in conjunction
    with additional `attrs` that are required.

    Instead of using subclassing we lift the attrib()s from the base class and
    reorder them as we need
    """
    if base is None:
        name = f"{topic.capitalize()}Config"
        base = object
    else:
        name = base.__name__ + '_Augmented'
        # retain all processors defined in the base class - do not overwrite
        field_processors.update({X.name: Attribute_to_attrib(X)
                                 for X in attr.fields(base)})

    ensure_processors_are_in_valid_order(field_processors)
    field_processors = ensure_processors_are_valid_attr_names(field_processors)
    return attr.make_class(name, attrs=field_processors)


def ensure_processors_are_valid_attr_names(field_processors):
    new_processors = {}
    for proc in field_processors:
        new_key = proc

        if not new_key.isidentifier():
            # Attempt to make attribute name valid
            new_key = re.sub(r'[\W]+', '', new_key)

        # Check first character in string, if not letter or underscore,
        # insert lowercase a at beginning, or if name is a python keyword
        if iskeyword(new_key) or (not new_key[0].isalpha() and not new_key[0] == '_'):
            new_key = 'a' + new_key
        new_processors[new_key] = field_processors[proc]
    return new_processors


def ensure_processors_are_in_valid_order(processors):
    """
    Ensure fields are in a valid order to avoid errors.

    In Python 3.6+ dictionaries are ordered. As a result, the attr library uses
    this ordering to determine the order attributes are defined.

    This can create problems if attributes with default values are added before
    attributes without. This roughtly transltes to the following:

    class Foo:
        def __init__(self, bar=7, baz):
            self.bar = bar
            self.baz = baz

    Which yields: SyntaxError: non-default argument follows default argument
    """
    tmp = {}
    # `attribute` is a _CountingAttr (intermediate representation)
    # the default value we seek is _default
    # https://github.com/python-attrs/attrs/blob/master/src/attr/_make.py

    for name, attribute in list(processors.items()):
        if attribute._default is not attr.NOTHING:
            tmp[name] = attribute
            del processors[name]
    processors.update(tmp)


def Attribute_to_attrib(a):
    """
    Converts an attr.Attribute to an attr.attrib

    This is necessary because attr.fields and attr.fields_dict return
    attr.Attribute instances, but attr.attrib() returns a _CountingAttr

    attr.make_class requires _CountingAttr instances
    """
    return attr.attrib(default=a.default,
                       validator=a.validator,
                       repr=a.repr,
                       eq=a.eq,
                       hash=a.hash,
                       init=a.init,
                       type=a.type,
                       converter=a.converter,
                       metadata=a.metadata)


def default_field_values(field):
    default_values = []
    if field.converter:
        # values pandas.read_csv can return if no input is provided
        for input in [float('NaN'), '']:
            try:
                val_on_nan_input = field.converter(input)
            except Exception:
                pass
            else:
                default_values.append(val_on_nan_input)
    return default_values


def attr_items(attr_cls_instance):
    """
    Yields (attr, value, value_is_default) for an attr class instance.

    Similar to attr.asdict but with important differences:
    * attr.asdict will convert tuple values into lists
    * provides bool to explain if `value` is a default or user-provided
    """
    for field in attr.fields(type(attr_cls_instance)):
        name = field.name
        value = getattr(attr_cls_instance, name)
        yield name, value, value in default_field_values(field)

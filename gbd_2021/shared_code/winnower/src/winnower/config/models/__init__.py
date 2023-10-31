"""
The online spreadsheets are primarily processed entirely as text field,
ignoring any in-built formatting to the spreadsheet cells.

Normally forms are used to marshall data between the internal data
representation and the state represented on the form. In this case only
marshalling from form state (free-form text) to internal representation is
required.

For users of frameworks like wtform, django, or Ruby on Rails there is a
familar pattern of automating work as follows

1) define a model containing one or more fields
2) define a form referencing the model
3) let the framework introspect the model and auto-generate a default form
4) if necessary, override auto-generated form fields to customize behavior
    to e.g., use a different visual widget or validate a reasonable age.

winnower uses a conceptually similar system that uses data instead of code
classes. Our indicator (model) definitions are retrieved from this
data and used to dynamically generate form classes to process the
spreadsheet-based configuration.

It is important to note that the forms we produce are usually multi-forms
(a form made up of multiple sub-forms). This is because it is more convenient
to input data in a single spreadsheet for all Indicators in e.g., the education
topic, instead of using one spreadsheet *per Indicator* in the education topic.

Adding a further wrinkle, because we use spreadsheets users frequently include
additional columns in the spreadsheet (e.g., "Note"). We choose to include this
information as-is during form processing rather than try and remove it.

sub-modules:
    fields.py       Provides basic field types and helper methods.
    topic_forms.py  Forms for specialized processing of specific topics, like
                    demographics or design. Also, the TopicConfigManager which
                    is a form discovery mechanism.
    indicators.py   Provides method `fields_for_indicator`, which returns a
                    dict of {field_name: form_field_processor} for each field
                    related to an indicator.
    reflect.py      Builds forms ready to process configuration by integrating
                    1) appropriate topic form (if defined)
                    2) appropriate fields based off of used indicators
                    3) any other field names present in the configuration
    setup_forms.py  Static form definitions for unchanging parts of internal
                    machinery.
"""
from .fields import required_fields  # noqa
from .reflect import attr_items, get_form  # noqa
from . import setup_forms  # noqa

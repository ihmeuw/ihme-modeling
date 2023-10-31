#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :
"""
Base Component class. Refactored to it's own module to avoid circular imports.
"""
from abc import (
    ABC,
    abstractmethod,
)

from attr import attrs, attrib

from winnower.logging import get_class_logger


class OpaqueComponent(ABC):
    """
    The OpaqueComponent is the base class for all transforms.

    A transform is a class that is configured upon instantiation and, later,
    provided a pandas.DataFrame. It will then perform a logical transformation
    such as computing a new column on that DataFrame and then return a
    DataFrame.

    An opaque component provides no information about what processing it will
    do. It may optionally validate whether it expects to be able to run
    successfuly based on the presence of input columns.
    """
    # replace this with a dict of metadata regarding the execution of a
    # component after .execute() is called
    # Example: FilterComponent might record # of input and output rows.
    _execute_metadata = None

    def validate(self, input_columns):  # TODO: docstring upadte
        """
        Validates this component can run.

        Args:
            source: the FileSource or Component providing data to this.
                This same argument is expected to be provided to execute()

        raises errors.ValidationError if it cannot.
        """
        if hasattr(self, '_validate'):
            self._validate(input_columns)

    @abstractmethod
    def execute(self, df):
        """
        Execute this transform and return the result data.

        In addition, store metadata on the execution run.

        Returns a DataFrame
        """

    def execute_metadata(self):
        """
        Return metadata regarding the execution of this component.

        Returns a dict, or None if execute() has not been called.
        """
        return self._execute_metadata


class Component(OpaqueComponent):
    """
    A Component must advertise how it effects a DataFrame.

    This is accomplished in two ways that differ from an OpaqueComponent:
        - A component must define _validate
        - A component must define what the columns will be output given a list
          of columns that will be present in `df` during execute().
    """
    @abstractmethod  # mandate method by declaring abstract
    def _validate(self, input_columns):
        """
        Validates this component can run.

        raises errors.ValidationError if it cannot.
        """

    @abstractmethod
    def output_columns(self, df_input_columns):
        """
        Return sequence of str values indicating output columns.

        Example behaviors:
            Subset: presist the input columns, neither adding nor removing.
            Rename: remove a single column and add a new column.
            Drop: remove one or more columns.
            Alias: add a new column.
        """


def attrs_with_logger(cls):
    """
    Wraps attrs decorator to add a 'logger' attribute.
    """
    default_logger = get_class_logger(cls)
    cls.logger = attrib(default=default_logger,
                        repr=False,
                        eq=False,
                        hash=False)

    return attrs(cls)

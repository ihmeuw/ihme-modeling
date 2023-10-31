"""
Base class for custom code transformations.
"""
from abc import (
    ABC,
    abstractmethod,
)

import attr
import pandas

from winnower import errors
from winnower.extract import get_column
from winnower.logging import get_class_logger


class TransformBase(ABC):
    # TODO: document that subclasses should be named "Transform"
    def __init__(self, columns, config, extraction_metadata):
        # Create a copy of output_columns() here, as chain is stateful and
        # subsequent transforms will modify this result
        self.columns = columns
        self.config = config
        self.logger = get_class_logger(self)
        self.extraction_metadata = extraction_metadata

    def get_column_name(self, name):
        """
        Returns the actual column name given the case-insensitive match `name`.
        """
        column, _, err = get_column(name, self.columns)
        if column is None:
            raise errors.ValidationError(err.args[0])
        return column

    @abstractmethod
    def execute(self, df) -> pandas.DataFrame:
        """
        Executes transformation and returns resulting data frame.
        """

    # API compatible with winnower.transform.base.Component
    # TODO: Implement both validate() and output_columns() as
    # abastractmethods for an attempt to mandate validations
    # for ValidatedExtractionChain
    # @abstractmethod # not all topics implement them yet
    def validate(self, input_columns):
        """
        Validates that this component can run.
        """

    def get_uses_columns(self):
        return getattr(self, 'uses_extra_columns', [])

    # @abstractmethod # not all topics implement them yet
    def output_columns(self, input_columns):
        """
        Return columns to be output.

        Most Transforms will only add columns or persist the existing columns.
        """

    def record_uses_column_if_configured(self, column):
        """
        Adds column/s to uses_extra_columns for an indicator if
        present (case insensitive match) in self.columns.
        """
        unresolved_column = self.config.get(column)
        if unresolved_column:
            if isinstance(unresolved_column, str):
                resolved = self.get_column_name(unresolved_column)
                self.uses_extra_columns.append(resolved)
            elif isinstance(unresolved_column, (tuple, list, frozenset)):
                for col in unresolved_column:
                    resolved = self.get_column_name(col)
                    self.uses_extra_columns.append(resolved)
            else:
                msg = f"Unknown type: {type(unresolved_column)} for config {column}"  # noqa
                raise errors.Error(msg)


@attr.s
class ExtractionMetadata:
    """
    Read-only interface for holding data about an extraction.

    Used as a more limited means of sharing information than e.g., providing a
    reference to an UbcovExtractor to TransformBase.__init__
    """
    indicators = attr.ib()
    indicator_config = attr.ib()
    topic_config = attr.ib()

    @classmethod
    def from_UbcovExtractor(cls, ubcov_extractor):
        return cls(
            indicators=ubcov_extractor.indicators,
            indicator_config=ubcov_extractor.get_indicator_config(),
            topic_config=ubcov_extractor.topic_config,
        )

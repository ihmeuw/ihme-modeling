import collections
import re

from winnower import errors
from winnower.logging import (
    log_case_mismatch,
    get_class_logger,
)

from winnower.globals import eg  # Extraction context globals.


def get_merges_from_chain(chain):
    """
    Walks the transformation chain search for Merge transforms and collects
    them.
    """
    # Causes circular import.
    from winnower.transform import Merge
    merges = []
    for t in chain:
        # get all the merges from the chain.
        if isinstance(t, Merge):
            merges.append(t)
    return merges


class BaseExtractionChain:
    def __init__(self, source):
        self._source = source
        self._chain = collections.deque()
        # cached copy of current output_columns. Stored as immutable value.
        self._oc = tuple(source.output_columns())
        self.logger = get_class_logger(self)
        self.added_cols = list()

    def __iter__(self):
        chains = collections.deque([self._chain])
        src = self._source
        while isinstance(src, BaseExtractionChain):
            chains.appendleft(src._chain)
            src = src._source

        for chain in chains:
            yield from chain

    @property
    def source(self):
        "Returns the primary data source for this extraction chain."
        source = self._source
        if isinstance(source, BaseExtractionChain):
            source = source.source
        return source

    def execute(self):
        """
        Load source data and performs each transform, returning a dataframe.
        """
        df = self._source.execute()

        # When True enables more detailed debugging
        if eg.more_debug:
            self.logger.debug(f"After source transform shape: {df.shape}")
            merges = get_merges_from_chain(self._chain)
            for m in merges:
                self.logger.debug(f"Merge cols left: {m.left_cols} "
                                  f"right: {m.right_cols}")

        for transform in self._chain:
            # Detailed debugging
            if eg.more_debug:
                t_name = getattr(type(transform),
                                 "__name__",
                                 "unknown transform")
                self.logger.debug(f"Before transform {t_name}, "
                                  f"shape is {df.shape}")
                self.logger.debug(f"Transform: {transform}")

            df = transform.execute(df)
            if eg.more_debug:
                t_name = getattr(type(transform),
                                 "__name__",
                                 "unknown transform")
                self.logger.debug(f"After transform {t_name}, "
                                  f"shape is {df.shape}")

        # reset index to a simple range; it has no external meaning
        return df.reset_index(drop=True)

    def get_input_column(self, column_name, *, log=True):
        """
        Returns the actual column from chain given `column_name`.

        ubCov configuration uses all lower-case columns regardless of the
        names in the extracted file. This has caused issues in ubCov (in Stata
        "IN" and "In" are valid names, but "in" is a reserved name).

        To excourage explicitness and avoid processing difficulties winnower
        does not convert all column names to lower case. As a result, a case-
        insensitive match is required.

        Args:
            chain: the extraction chain to retrieve the column of.
            column_name: provided column name.
        Raises:
            ValidationError if no match can be found

        Returns the discovered column name.
        """
        # Validate from_name exists in chain
        column, imatch, err = get_column(column_name, self.output_columns())

        if column is None:
            raise errors.ValidationError(err.args[0])

        if imatch and log:
            log_case_mismatch(column, column_name)

        return column

    def output_columns(self):
        return list(self._oc)


class ValidatedExtractionChain(BaseExtractionChain):
    """
    Represents a logical sequence of operations to perform an extraction.

    A validated extraction chain encapsulates a data source and a sequence of
    transformations made to that data source. Each transformation is expected
    to provide information about it's behavior by way of the validate() and
    output_columns() methods.

    A validationed extraction chain should never error for preventable reasons
    (as opposed to e.g., I/O failure).
    """
    def append(self, transform):
        transform.validate(self._oc)
        self._chain.append(transform)
        # cache values
        try:
            input_cols = self.output_columns()
            # Make a copy before passing to output_columns because
            # it will mutate input_cols_copy in place
            input_cols_copy = list(input_cols)
            new_output_cols = transform.output_columns(input_cols_copy)
            self.added_cols.extend(X for X in new_output_cols
                                   if X not in input_cols)
            self.add_to_use_columns(transform)
            self._oc = tuple(new_output_cols)
        except AttributeError as e:
            if e.args[0].endswith("object has no attribute 'output_columns'"):
                msg = ("Cannot provide OpaqueComponents to "
                       "ValidatedExtractionChain")
                msg = (f"transform {transform.__class__.__name__!r} has no "
                       "output_columns method. Must provide non-opaque "
                       "transforms to ValidatedExtractionChain.")
                raise errors.ValidationError(msg) from None
            else:
                raise

    def add_to_use_columns(self, transform):
        """
        Updates self.source.uses_columns with all new columns transform needs,
        that are not provided by previous transforms in the extraction chain.
        Each transform component returns the list of needed columns via
        transform.get_uses_columns().
        """
        source_cols = self.source.uses_columns
        try:
            extra_columns = transform.get_uses_columns()
        except AttributeError:
            msg = (f"transform {transform.__class__.__name__!r} has no "
                   "get_uses_columns method.")
            raise errors.Error(msg)
        else:
            if extra_columns:
                if not isinstance(extra_columns, list):
                    msg = ("get_uses_columns() expected to return list "
                           f"but returned: {type(extra_columns)}.")
                    raise TypeError(msg)
                # Adding extra_columns to uses_columns for the first time
                if not source_cols:
                    source_cols = list()

                # if extra_columns are collected from merge's
                # right_chain.source, add to it's uses_columns
                add_columns_to_merge_right_chain(self, extra_columns=extra_columns)  # noqa

                source_cols.extend(
                    X for X in extra_columns
                    if X not in source_cols
                    and X not in self.added_cols)

                self.source.uses_columns = source_cols


class ExtractionChain(BaseExtractionChain):
    """
    Represents a logical sequence of operations to perform an extraction.

    An extraction chain encapsultes a data source OR a ValidatedExtractionChain
    (which is effectively a data source) and a sequence of transformations to
    apply to that data source.

    Unlike a ValidatedExtractionChain an ExtractionChain makes no attempt to
    guarantee runtime success. Instead it simply loads it data and executes
    each transformation one at a time. By omitting the validation steps users
    are empowered to provide transformations in the form of less complex code
    that "just works" without bearing the burdon of providing validation
    information.
    """
    def append(self, transform):
        transform.validate(self._oc)
        self._chain.append(transform)
        if hasattr(transform, 'output_columns'):
            try:
                output_cols = transform.output_columns(self.output_columns())
            except NotImplementedError:
                pass
            else:
                self._oc = output_cols


class ExtractionChainBuilder:
    """
    Appends transform to ValidatedExtractionChain / ExtractionChain.
    It is an additional validation layer before the transforms get
    appended to the chain.

    Args:
        chain: ExtractionChain or ValidatedExtractionChain
    """

    def __init__(self, chain):
        self.chain = chain

    def append(self, transform):
        """
        Validates and collects columns in merge's right_chain
        before passing on to chain for append.
        Args:
            transform: Any transform that needs to append to chain
            config_dict: Indicators and its configuration from UbCovExtractor
        """
        # avoids circular imports
        from winnower.transform import Merge

        if isinstance(self.chain, ExtractionChain) or \
                not self._transform_is_validated(transform):
            # If appending to ExtractionChain, set uses_columns to
            # None which ensures all columns are loaded
            if self.chain.source.uses_columns:
                self.chain.source.uses_columns = None

            # If the chain is non-validated (ExtractionChain) or transform
            # to append is non-validated, the right_chain
            # for merge should be non-validated as well because the later
            # transforms don't have access to append to right_chain.
            invalidate_cols_collect_on_merge_right_chain(self.chain)

            if isinstance(self.chain, ValidatedExtractionChain):
                self.chain = ExtractionChain(self.chain)
        else:
            # chain is validated and the transform is validated.

            # NOTE: right_chain handles its column collection on its own.
            # Following is the case where additional column collection
            # is required on right_chain
            if isinstance(transform, Merge) and isinstance(transform.right_chain, ValidatedExtractionChain):  # noqa
                merge = transform
                merge_rc = transform.right_chain
                right_extra_columns = set()
                right_extra_columns.update(merge.right_cols)

                # If columns_to_label are in source of right_chain
                # collect them
                if merge.column_labels:
                    for configured_col in merge.column_labels:
                        col = merge_rc.get_input_column(configured_col)
                        right_extra_columns.add(col)

                if merge_rc.source.uses_columns:
                    right_extra_columns.update(merge_rc.source.uses_columns)  # noqa
                col_list = list(right_extra_columns)
                merge_rc.source.uses_columns = col_list
        self.chain.append(transform)

    def get_input_column(self, column_name, *, log=True):
        """
        Wrapper for get_input_column() in BaseExtractionChain
        """
        return self.chain.get_input_column(column_name=column_name, log=log)  # noqa

    def output_columns(self):
        """
        Wrapper for output_columns() in BaseExtractionChain
        """
        return self.chain.output_columns()

    @property
    def source(self):
        """
        Wrapper for source() in BaseExtractionChain
        """
        return self.chain.source

    def get_chain(self):
        return self.chain

    def _transform_is_validated(self, transform):
        """
        If a transform cannot confidently advertise which columns
        it requires for its operation, it is not validated.
        """
        # avoiding cicular imports
        from winnower.custom.base import TransformBase
        from winnower.extract_hooks import (
            HookTransform,
            OpaqueHookTransform,
        )
        from winnower.transform.components import Subset

        if isinstance(transform, HookTransform):
            return hasattr(transform, 'hook_cls') and \
                    hasattr(transform.hook_cls, 'USES_EXTRA_COLUMNS')
        elif isinstance(transform, OpaqueHookTransform):
            return False
        elif isinstance(transform, TransformBase):
            return hasattr(transform, 'uses_extra_columns')
        elif isinstance(transform, Subset):
            # cannot confidently collect columns for Subset
            return False
        else:
            return True


def add_columns_to_merge_right_chain(chain, extra_columns):
    """
    Manages column collection for right_chain of Merge in an attempt to
    selectively load column.

    Args:
        chain: ValidatedExtractionChain
        extra_columns: columns required by a transform. extra_columns
                       is not in scope for non-validated ExtractionChain
    """
    merges = get_merges_from_chain(chain)
    for merge in merges:
        if isinstance(merge.right_chain, ValidatedExtractionChain):
            rc_source = merge.right_chain.source
            if not rc_source.uses_columns:
                rc_source.uses_columns = list()

            rc_source.uses_columns.extend(
                X for X in extra_columns
                if X not in rc_source.uses_columns
                and X in rc_source.output_columns())


def invalidate_cols_collect_on_merge_right_chain(chain):
    """
    Invalidates right_chain of all merges for efficient column
    loading. Makes sure that the whole data is loaded on right_chain.

    Args:
        chain: ExtractionChain or ValidatedExtractionChain
    """
    merges = get_merges_from_chain(chain)
    for merge in merges:
        if isinstance(merge.right_chain, ValidatedExtractionChain):
            if merge.right_chain.source.uses_columns:
                merge.right_chain.source.uses_columns = None
            merge.right_chain = ExtractionChain(merge.right_chain)


def get_column(column, columns):
    """
    Returns str column value in `columns`.

    Performs case-insensitive lookup if necessary.

    Returns (colname, imatch, err)

    if colname is not None:
        the column was found, and the colname is the found value.
        imatch is a bool indicating if it was an inexact match
        err is None
    if colname is None:
            imatch is None
            err is either NotFound (zero matches) or Error (2+ matches)
    """
    is_imatch = re.compile(f'^{column}$', re.IGNORECASE).match
    imatches = []
    for c in columns:
        if column == c:
            return column, False, None
        elif is_imatch(c):
            imatches.append(c)

    if len(imatches) == 1:
        # TODO: LOG here, or elsewhere?
        return imatches[0], True, None

    if imatches:
        msg = (f"Multiple case-insensitive matches for {column} in {columns} "
               f"{imatches}. Please provide exact column name.")
        err = errors.Error(msg)
    else:
        err = errors.NotFound(f"No match for {column!r} in {columns}")
    return None, None, err

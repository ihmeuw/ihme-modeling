"""
Errors that can be raised by winnower.
"""


class Error(Exception):
    """Base error class. All errors raised by winnower should inherit this."""


class ValidationError(Error):
    """An error occurred validating a component."""


class NullError(Error):
    """
    A Null value was encountered that could not be replaced/fixed.

    Null refers to any value that pandas.isnull() returns True for.
    """


class NotFound(Error):
    "A value was not found."


class RequiredInputsMissing(Error):
    """A required input, probably a column, isn't present."""


class LoadError(Error):
    """Loading of a file failed."""


class MultipleValues(Error):
    """A single value was requested, but multiple values were present."""


class InvalidExpression(Error):
    """
    An expression was used that was invalid.

    A valid expression might be "> 5". An invalid expression might be " < r".
    """


class DecorationError(Error):
    "A decorator was used incorrectly."


class AllAssertionErrors(Error, AssertionError):
    """
    Provide a clean summary of all errors after assertion
    Missing Columns
    # columns are in 'expected' but not in 'actual':
      - <column>
      - ...
    Extra Columns
    # columns are in 'actual' but not in 'expected':
      - <column>
      - ...
    Unequal Columns
    # columns didn't compare:
    (#) <column>
    <error_message for that column>
    ...
    """
    def __str__(self):
        assert_errors_dict, *rest = self.args
        assert_errors_df = assert_errors_dict['directory'].astype(str)
        # ubcov id 45
        # if 45 is a 'wrong_col' both columns exist, values don't compare then
        # for 45, there is a corresponding distribution, 'err_frame' this
        # distribution then is passed to _dist_frame_to_string to print out

        # distribution error first does not include missing or extra columns
        # a subset of AssertionErrors
        # it includes the AssertionErrors that can be displayed in a frequency
        # table that is not obnoxiously long

        err_dist_dict = assert_errors_dict['distribution']
        msg = ''
        aed_m = assert_errors_df[assert_errors_df.error_type == 'missing_col']
        aed_e = assert_errors_df[assert_errors_df.error_type == 'extra_col']
        # DistributionErrors are subset of this group
        aed_w = assert_errors_df[assert_errors_df.error_type == 'wrong_col']
        aed_k = assert_errors_df[assert_errors_df.error_type == 'known_issue']

        msg += self._print_info(aed_m, "\nMissing {} columns from expected")
        msg += self._print_info(aed_e, "\nFound {} columns not in expected")
        msg += self._print_info(aed_w, "\n{} unequal columns don't compare",
            err_dist_dict)
        msg += self._print_info(aed_k, "\n{} known issues we are okay with")
        # pdb.set_trace()
        return msg

    def _print_info(self, aed_sub, message, diff_dist=None):
        if aed_sub.empty:
            return ''
        msg = message.format(len(aed_sub))
        type = aed_sub.error_type.unique()[0][0]
        for col in aed_sub.iterrows():
            column = col[1].column
            msg += "\n  " + column.upper()
            if col[1].details != 'nan':
                msg += ': ' + col[1].details
            if type in ['e', 'w', 'k']:
                msg += self._print_col_info(col[1], 'a')
            if type in ['m', 'w', 'k']:
                msg += self._print_col_info(col[1], 'e')
            if type == 'w':
                # insert the print whether or not columns compare
                msg += self._dist_frame_to_string(diff_dist[column])
        return msg

    def _print_col_info(self, col, p):
        new_msg = '\n  winnower output' if p == 'a' else '\n  ubcov output'
        pt = "\n  - "
        q = 'e' if p == 'a' else 'a'
        new_msg += pt + col[p + '_num_nulls'] + ' out of ' \
            + col[p + '_num_obs'] + ' null values'
        new_msg += pt + col[p + '_num_unique_vals'] + ' unique values'
        x = col[f"num_unique_{p}_not_in_{q}"]
        if x != 'nan':
            new_msg += f"({x} not in {q})"
            new_msg += pt + f"sample vals not in {q}: " \
                + col[f"sample_{p}_not_in_{q}"]
        else:
            new_msg += pt + 'ex: ' + col[p + '_sample_vals']
        return new_msg

    def _dist_frame_to_string(self, frame):
        frame = frame.head(10) if len(frame) > 20 else frame
        frame.index = [repr(X) for X in frame.index]
        return '\n    ' + frame.to_string().replace('\n', '\n    ')

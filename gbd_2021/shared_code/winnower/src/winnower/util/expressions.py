"""
Expressions relating to data.

The original ubCov implementation allowed configuration in places that
implicitly referenced a value in a boolean expression. Examples include ">0"
and ">0 & < 95"

This includes methods to assist in dealing with these expressions.
"""
import re

from winnower import errors


def is_expression(config_vals: list):
    """
    Predicate indicating if "config_vals" is an expression.

    One such expression might be ["> 99"]
    """
    if len(config_vals) != 1:
        # expressions may be compounded via '&' symbols, but not via ','s
        return False
    exp, = config_vals
    if not isinstance(exp, str):
        return False
    return bool(re.search('>|<', exp))


def build_expression_query(config_vals: list, colname: str):
    if len(config_vals) > 1:
        msg = f"expression can contain only one statement: got {config_vals!r}"
        raise errors.Error(msg)

    if not is_expression(config_vals):
        raise errors.Error(f"{config_vals} is not an expression")

    pieces = [X.strip() for X in config_vals[0].split('&')]
    # the column name is implicit in the statement
    return " & ".join(f"{colname} {piece}" for piece in pieces)

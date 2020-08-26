# A set of dynamic tupe validation functions. generally they chek that a value
# is not None and of the right type,  and raise a good error message if it is
# not correct.


def is_int(var, name):
    if (var is not None) and isinstance(var, int):
        return var
    else:
        raise ValueError("{n} must be non-null and an integer, not '{v}'"
                         .format(n=name, v=var))


def is_None_or_positive_int(var, name):
    if var is None:
        return var

    if (var is not None) and isinstance(var, int) and var > 0:
        return var
    else:
        raise ValueError(
            "{n} must be non-null and a positive integer, not '{v}'"
            .format(n=name, v=var))


def is_positive_int(var, name):
    if (var is not None) and isinstance(var, int) and var > 0:
        return var
    else:
        raise ValueError("{n} must be a positive integer, not '{v}'"
                         .format(n=name, v=var))


def is_lower_bounded_int(var, lower_bound, name):
    if (var is not None) and isinstance(var, int) and var >= lower_bound:
        return var
    else:
        raise ValueError("{n} must be an integer greater than or equal to "
                         "{b}, not '{v}'".format(n=name, b=lower_bound, v=var))


def is_list_of_int(var, name):
    if (var is not None) and isinstance(var, list):
        for v in var:
            is_int(v, "Non-integer element in list of integers: {}:"
                   .format(name))
        return var
    else:
        raise ValueError("{n} must not be None, and also a list, not '{v}'"
                         .format(n=name, v=var))


def is_None_or_list_of_int(var, name):
    if var is None:
        return var

    if (var is not None) and isinstance(var, list):
        for v in var:
            is_int(v, "Non-integer element in list of integers: {}:"
                   .format(name))
        return var
    else:
        raise ValueError("{n} must not be None, and also a list, not '{v}'"
                         .format(n=name, v=var))


def is_list_of_year_ids(var, name):
    if (var is not None) and isinstance(var, list):
        for v in var:
            is_int(v, "Non-integer element in list of year_ids {}:"
                   .format(name))
            if v < 1900 or v > 2100:
                raise ValueError("{n}: all elements must be a year in "
                                 "[1900..2100], not '{v}'"
                                 .format(n=name, v=var))
        return var
    else:
        raise ValueError("{n} must not be None, and also a year in "
                         "[1900..2100], not '{v}'".format(n=name, v=var))


def is_string(var, name):
    if (var is not None) and isinstance(var, str):
        return var
    else:
        raise ValueError("{n} must be non-null and a string, not '{v}'"
                         .format(n=name, v=var))


def is_boolean(var, name):
    if (var is not None) and isinstance(var, bool):
        return var
    else:
        raise ValueError("{n} must be non-null and a boolean, not '{v}'"
                         .format(n=name, v=var))


def is_best_or_positive_int(var, name):
    failed = True
    if (var is not None):
        if var == 'best':
            failed = False
        if isinstance(var, int) and var > 0:
            failed = False
    if failed:
        raise ValueError("{n} must be a positve_int or 'best'. "
                         "Passed: '{v}'".format(n=name, v=var))
    else:
        return var

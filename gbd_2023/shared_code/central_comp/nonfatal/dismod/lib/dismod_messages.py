import logging
import re


def log_exceptional_messages(message: bytes) -> None:
    """Dismod programs write messages to stderr. Some of these messages are
    so common that they are uninformative. We strip these out. But if there
    are any messages left after we strip uninformative ones, we log these
    """
    exceptional = filter_messages(message)
    if exceptional:
        log = logging.getLogger(__name__)
        log.warning(exceptional)


def filter_messages(message: bytes) -> str:
    """
    Take stderr output from dismod and remove substrings that we don't want
    to bother recording.
    """
    filters = [_remove_progress_bar, _remove_effect_override]
    filtered = message.decode()
    for func in filters:
        filtered = func(filtered)
    return filtered


def _remove_progress_bar(string: str) -> str:
    """All dismod messages include a progress bar. Since we don't write stderr
    output until after the program is finished, the progress bar is not helpful.

    The progress bar looks something like this:
        mcmc_chain: num_watch = 100
    1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19
    20 ...rest omitted... 98 99
    """
    # match any number of repeating digits with varying spaces or newlines
    # between them
    regex = r"mcmc_chain: num_watch = (?:[ ]*\d+[\n\r ]+\d+\n?)+"
    return re.sub(regex, "", string)


def _remove_effect_override(string: str) -> str:
    """If there is data for a location that is missing from , dismod
    will automatically change the effect intervals for that location to
    upper = lower (ie disable/remove that location).

    See 
    This looks like:
    dismod_ode: Warning: integrand = mtother: super-region = 31
        does not appear in 
        lower = -1 upper = 1 in 
        changed to lower = upper = 0.0
    dismod_ode: Warning: integrand = mtother: super-region = 4
        does not appear in 
        lower = -1 upper = 1 in 
        changed to lower = upper = 0.0
    """
    regex = (
        r"dismod_ode: Warning: integrand = [a-z]+: [-a-z]+ = \d+[\t\r\n ]+does "
        r"not appear in [/_.a-z\d]+[\t\n ]+lower = [-.\d]+ upper = [-.\d] in "
        r"[/_.a-z\d]+[\t\n ]+changed to lower = upper = 0.0[\t\n\r]?"
    )
    return re.sub(regex, "", string)

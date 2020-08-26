from argparse import ArgumentParser
from pathlib import Path

from cascade_ode.constants import Methods


METHOD_NAMES = [Methods.DISABLE_NON_STANDARD_LOCATIONS,
                Methods.ENABLE_EMR_CSMR_RE,
                Methods.ENABLE_HYBRID_CV]

ARGUMENTS = dict(
    pdb=dict(
        flags=["--pdb"],
        action="store_true",
        help=("Open a debugger when there is an exception. "
              "From the command line, running with --pdb means "
              "that if the code hits an exception it will drop "
              "into the debugger."),
    ),
    no_upload=dict(
        flags=["--no-upload"],
        action="store_true",
        help="Do not upload anything to databases. All uploads turned off."
    ),
    config_file=dict(
        flags=["--config-file"],
        action="store",
        type=Path,
        help=("The argument to this option is the full path to a "
              "configuration file that has the same entries as "
              "config.local. Use it to run with a different "
              "base directory.")
    ),
    verbose=dict(
        flags=["-v", "--verbose"],
        action="count",
        default=0,
        help=("Increase logging verbosity. Different parts of the code may "
              "have different logging verbosity, and each -v increases "
              "verbosity.")
    ),
    quiet=dict(
        flags=["-q", "--quiet"],
        action="count",
        default=0,
        help=("Decrease logging verbosity. Different parts of the code may "
              "have different logging verbosity, and each -q decreases "
              "verbosity.")
    ),
    disable_nonstandard_locations=dict(
        flags=[
            f"--{Methods.DISABLE_NON_STANDARD_LOCATIONS.replace('_', '-')}"],
        dest=Methods.DISABLE_NON_STANDARD_LOCATIONS,
        default=None,
        action="store_true",
        help=("Turns off extraction and application of betas for standard "
              "locations.")
    ),
    no_disable_nonstandard_locations=dict(
        flags=[
            f"--no-{Methods.DISABLE_NON_STANDARD_LOCATIONS.replace('_', '-')}"
        ],
        dest=Methods.DISABLE_NON_STANDARD_LOCATIONS,
        default=None,
        action="store_false",
        help=("Turns on extraction and application of betas for standard "
              "locations.")
    ),
    enable_emr_csmr_re=dict(
        flags=[f"--{Methods.ENABLE_EMR_CSMR_RE.replace('_', '-')}"],
        dest=Methods.ENABLE_EMR_CSMR_RE,
        default=None,
        action="store_true",
        help="Turns on effects for mtspecific and mtexcess in gen_effect"
    ),
    no_enable_emr_csmr_re=dict(
        flags=[f"--no-{Methods.ENABLE_EMR_CSMR_RE.replace('_', '-')}"],
        dest=Methods.ENABLE_EMR_CSMR_RE,
        default=None,
        action="store_false",
        help="Turns off effects for mtspecific and mtexcess in gen_effect"
    ),
    enable_hybrid_cv=dict(
        flags=[f"--{Methods.ENABLE_HYBRID_CV.replace('_', '-')}"],
        dest=Methods.ENABLE_HYBRID_CV,
        default=None,
        action="store_true",
        help="Chooses minCV value from max of current cv, eta, or settings"
    ),
    no_enable_hybrid_cv=dict(
        flags=[f"--no-{Methods.ENABLE_HYBRID_CV.replace('_', '-')}"],
        dest=Methods.ENABLE_HYBRID_CV,
        default=None,
        action="store_false",
        help="Chooses minCV value from max of current cv or settings"
    ),
)
"""
The dictionary key is the name argparse gives to this argument and the value
is parameters with which to make the command-line argument.
Every argument has an action that is used by the ``inverse_parser`` below,
so if you add an option, be sure to set its action.

This can be used for feature-flagging, which means introducing a new feature
into the main development branch, but guarding it behind a command-line
flag that makes not using that feature the default. Later, change the
feature from store_true to store_false in order to make it the default
behavior. Later still, clean up past feature flags.
"""


def cascade_parser(description=""):
    """
    Create a default set of command-line arguments for applications in
    Cascade-ODE. Every script run by Jobmon has these flags, plus whatever
    else it adds.

    Args:
        description (str): This shows up on the command line as the program
            description text.
    """
    parser = ArgumentParser(description=description)
    for argument in ARGUMENTS.values():
        keywords = argument.copy()
        del keywords["flags"]
        parser.add_argument(*argument["flags"], **keywords)
    return parser


def inverse_parser(args):
    """Once arguments are parsed, this turns them back into a command line
    to pass to an SGE process. It picks out those arguments in the
    default set. For example, in the driver file::

        parser = cascade_parser()
        parser.add_argument("mvid")
        args = parser.parse_args()
        add_args_to_children = inverse_parser(args)

    Args:
        args (argparse.Namespace): Args from ``parse_args()``.

    Returns:
        List[str]: A possibly-empty list of strings to add to a
        command line that uses ``cascade_parser()``.
    """
    command_line = list()
    for name, argument in ARGUMENTS.items():
        if not hasattr(args, name):
            continue

        action = argument["action"]
        value = getattr(args, name)
        flag = argument["flags"][0]
        if "default" in argument:
            default = argument["default"]
        else:
            default = None

        if action == "count":
            command_line.extend([flag] * value)
        elif action == "store_true":
            if value:
                command_line.extend([flag])
        elif action == "store":
            if value != default:
                command_line.extend([flag, str(value)])
            # else don't add the default back.
        else:
            raise RuntimeError(
                f"Unknown argument action, {action} for {name}.")
    return command_line

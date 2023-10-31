import argparse
import logging
import os
import sys
from unittest.mock import patch

from winnower import arguments
from winnower.config.ubcov import (
    UrlPaths,
    UbcovConfigLoader,
)
from winnower import errors
from winnower.extract_hooks import (
    _HOOK_DIR,
    get_hooks,
)
from winnower.util.path import (
    fix_survey_path,
    LINUX_MOUNT_PATHS,
    OSX_MOUNT_PATHS,
)


def patched_fix_survey_path(*args, **kwargs):
    with patch.object(sys, "platform", "win32"):
        result = fix_survey_path(*args, **kwargs)

    if isinstance(result, str):
        if result.startswith(("/snfs1", "/ihme")):
            import pdb
            pdb.set_trace()

    return result


template = """\
from winnower.extract_hooks import ExtractionHook


class Fix{nid}_{winnower_id}(ExtractionHook):
    SURVEY_NAME = {survey_name!r}
    NID = {nid}
    IHME_LOC_ID = {ihme_loc_id!r}
    YEAR_START = {year_start}
    YEAR_END = {year_end}
    SURVEY_MODULE = {survey_module!r}
    FILE_PATH = "{file_path!s}"  # noqa

    # uncomment this to add code that would have gone in "subset" in ubCov
    # @ExtractionHook.pre_extraction.for_all_topics
    # def subset_query(self, df):
    #     return df

    # uncomment this to add code to run *immediately after* TOPIC custom code
    # NOTE: you have to replace "TOPIC" with your topic e.g., "wash"
    # @ExtractionHook.post_extraction.for_topics("TOPIC")
    # def post_extraction(self, df):
    #     return df
"""
logger = logging.getLogger('winnower.commands')


def entry_point():
    parser = argparse.ArgumentParser(
        description="Generate custom code for extraction hook")
    arguments.generate_extraction_hook_template(parser)
    args = parser.parse_args()
    arguments.set_arguments_log_level(args)

    config_root = UrlPaths.url_for(args.links_key)
    config_loader = UbcovConfigLoader.from_root(config_root)
    universal_tf = config_loader.get_tableframe("universal")

    universal = universal_tf.get(winnower_id=args.winnower_id)
    all_topics = list(config_loader
                      .get_tableframe("indicators")
                      .df
                      .topic_name
                      .dropna()
                      .unique())

    generate_extraction_hook_template(universal, all_topics,
                                      dry_run=args.dry_run)


def get_windows_path(file_path):
    """
    Returns a valid windows-based path given any path.

    Args:
        file_path: str path or pathlib.Path instance.

    This function is almost an inverse of winnower.util.path.fix_survey_path,
    which converts a Windows-based path to something appropriate for the
    platform.
    """
    result = str(file_path).replace('\\', '/')

    if sys.platform == "darwin":
        rev_path_map = OSX_MOUNT_PATHS
    elif sys.platform == "linux":
        rev_path_map = LINUX_MOUNT_PATHS
    else:
        rev_path_map = {}

    for win_root, runtime_root in rev_path_map.items():
        result = result.replace(runtime_root, win_root, 1)
    return result


def generate_extraction_hook_template(universal, all_topics, *, dry_run):
    win_path = get_windows_path(universal.file_path)

    code_block = template.format(
        winnower_id=universal.winnower_id,
        nid=universal.nid,
        survey_name=universal.survey_name,
        survey_module=universal.survey_module,
        ihme_loc_id=universal.ihme_loc_id,
        year_start=universal.year_start,
        year_end=universal.year_end,
        file_path=win_path)

    expected_path = _HOOK_DIR / f"nid_{universal.nid}.py"
    if expected_path.exists():
        try:
            # get all topics so we can ensure a match
            # NOTE: this is a poor method of getting all topics, but it should
            # work in practice as a topic without any idicators would not do
            # anything
            hooks = get_hooks(universal, all_topics, config_dict={})
        except errors.Error as e:
            logger.exception(e)
            logger.error("Error encountered when trying to check existing "
                         f"extraction hook file {expected_path}")
        else:
            if any(hooks):
                logger.info(
                    f"Your extract exists at {expected_path}. Just edit it.")
            else:
                # chomp first line (import statement)
                code_block = code_block.split("\n", 1)[1]
                if not dry_run:
                    with expected_path.open("a") as outf:
                        outf.write(code_block)
                logger.info(f"Appended template to {expected_path}")
    else:
        # set umask to 111 (AKA - everyone gets read/write permissions on file)
        prev_mask = os.umask(0o111)
        if not dry_run:
            with expected_path.open("w") as outf:
                outf.write(code_block)
        logger.info(f"Wrote template file {expected_path}")
        os.umask(prev_mask)

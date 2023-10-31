"""
List extraction ids.
"""
import argparse
import functools

from winnower import arguments
from winnower.commands.helpers import suppress_broken_pipes
from winnower.config.ubcov import (
    UrlPaths,
    UbcovConfigLoader,
)


@suppress_broken_pipes
def list_extraction_ids():
    parser = argparse.ArgumentParser(
        description='List all extraction ids, optionally for a topic')
    arguments.list_extraction_ids_arguments(parser)
    args = parser.parse_args()

    arguments.set_arguments_log_level(args)
    config_root = UrlPaths.url_for(args.links_key)
    config_loader = UbcovConfigLoader.from_root(config_root)

    topics = [X for X in args.topics if X not in arguments.DEFAULT_TOPICS]

    def get_ids(topic=None):
        "Return id values from configuration."
        method = config_loader.get_tableframe
        if topic is None:
            tf = method('universal')
        else:
            tf = method(topic, type='codebooks')
        return set(tf.df.ubcov_id.dropna().astype(int))

    if topics:
        ids = functools.reduce(set.intersection, map(get_ids, topics))
    else:
        ids = get_ids()

    for id in ids:
        print(id)

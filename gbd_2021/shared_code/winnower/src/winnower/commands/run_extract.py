import logging

import configargparse

from winnower import arguments
from winnower.config.ubcov import (
    UrlPaths,
    UbcovConfigLoader,
)
from winnower.util.string import UnicodeSimplifier
from winnower.util.categorical import downcast_float_to_integer


def run_extract(args=None, testing=False):
    parser = configargparse.ArgumentParser(
        default_config_files=['~/.winnower.ini'],
        description='Extract survey microdata for use in IHME processing.')
    arguments.run_extract_arguments(parser)
    args = parser.parse_args(args=args)
    if not testing:
        arguments.set_arguments_log_level(args)

    logger = logging.getLogger('winnower.commands')

    logger.info(f"\tTopics are {args.topics}")

    config_root = UrlPaths.url_for(args.links_key)
    ubcov_ids = args.ubcov_id
    # config_loader contains all the tables pulled from the ubcov databases
    # this should only happen once no matter how many ubcov_ids. Repeatedly
    # pulling these tables slows down the databases for other users and
    # causes web errors.

    config_loader = UbcovConfigLoader.from_root(config_root)
    basic_add_df = config_loader.data_frames['basic_additional']
    for id in ubcov_ids:
        logger.info(f"Running extraction for {id}")
        extractor = config_loader.get_extractor(id,
                                                topics=tuple(args.topics))
        file_id = get_file_id(basic_add_df, id)
        output_file = arguments.get_output_file(
            args, extractor.universal, extractor.merges, file_id)
        # save config before extraction (in case an error is raised)
        if args.save_config:
            arguments.save_config(args, extractor)

        extraction = extractor.get_extraction(
            keep_unused_columns=args.keep)
        df = extraction.execute()

        if args.remove_special_characters:
            df = UnicodeSimplifier.convert_unicode_characters_to_ascii(df)

        if not args.csv and not args.dta:
            # make dta the default output when no filetype is declared
            # in run_extract args
            args.dta = True

        if args.csv:
            df.to_csv(output_file.with_suffix('.csv'),
                      # index (row label) isn't necessary for any other use
                      index=False,
                      )

        if args.dta:
            def save():
                df.to_stata(output_file.with_suffix('.dta'),
                            write_index=False,  # index not useful
                            version=117)  # Stata 13 and newer
            try:
                save()
            except UnicodeEncodeError:
                df = UnicodeSimplifier.convert_unicode_characters_to_ascii(df)
                logger.error("Error saving unicode characters to ascii - "
                             "translating known diacritics and removing "
                             "remaining non-ascii characters")
                save()

        if args.runtime_directory != '.':
            msg = f"Saved to {args.runtime_directory}/{output_file}"
        else:
            msg = f"Saved to {output_file}"
        logger.info(msg)


def get_file_id(df, ubcov_id):
    """
    If there is a file_id for given ubcov_id in the dataframe, return it.
    Arguments:
        df: basic_additional dataframe.
        ubcov_id: user specified ubcov id.
    """
    df['ubcov_id'] = downcast_float_to_integer(df['ubcov_id'])

    if ubcov_id in df['ubcov_id'].unique():
        row_val = df.loc[df['ubcov_id'] == ubcov_id]
        return str(row_val.iloc[0]['file_id'])

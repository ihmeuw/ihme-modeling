import argparse
import logging
import os
import time
import pickle

from gbd import constants as gbd

from dalynator import dalynator_job_swarm as djs
from dalynator import app_common as ac
from dalynator import makedirs_safely as mkds
from dalynator import get_input_args


class DalynatorApplication(object):
    """In future there might be an Application base class."""

    def parse(self, tool_name="dalynator", cli_args=None):
        """
        Perform ALL argument parsing - argparse parsing, setting of defaults,
        checking of argument consistency. NO side effects in the file system.

        Args:
            tool_name: 'dalynator' or 'burdenator'
            cli_args: None or a list of strings. If None then read use sys.argv
        """
        parser = get_input_args.construct_parser_run_all_tool(tool_name)
        args = parser.parse_known_args(cli_args)
        if args[0].resume:
            print("Nator run in resume mode. Reading args from file")
            args = get_input_args.load_args_from_file(args, tool_name)
            args = get_input_args.set_phase_defaults(args)
            return args
        parser = get_input_args.add_non_resume_args(parser, tool_name)
        args = parser.parse_args(cli_args)
        args = get_input_args.set_phase_defaults(args)
        args.tool_name = tool_name

        if args.codcorrect_version == 'best':
            args.codcorrect_version = ac.best_version(
                'codcorrect', args.gbd_round_id, args.decomp_step)
        if args.fauxcorrect_version == 'best':
            args.fauxcorrect_version = ac.best_version(
                'fauxcorrect', args.gbd_round_id, args.decomp_step)
        if args.epi_version is None:
            args.epi_version = ac.best_version('como', args.gbd_round_id,
                                               args.decomp_step)

        args.sge_project = ac.create_sge_project(args.sge_project,
                                                 args.tool_name)

        # checking to make sure that mixed_draws contains the right info
        if args.mixed_draw_years is None:
            n_draws_years_dict = {args.n_draws: args.years}
        else:
            ac.check_mixed_draw_years_format(args.mixed_draw_years)
            n_draws_years_dict = args.mixed_draw_years

        year_n_draws_map = ac.construct_year_n_draws_map(n_draws_years_dict)

        ac.validate_multi_mode_years(
            year_n_draws_map, n_draws_years_dict, args.start_year_ids,
            args.end_year_ids)

        args.out_dir, args.log_dir, args.cache_dir = (
            get_input_args.construct_extra_paths(args.out_dir_without_version,
                                                 args.log_dir, args.tool_name,
                                                 args.output_version))
        return args

    def parse_and_initialize(self, cli_args=None):
        """
        Convenience for testing
        :param args_strings:  None or a list of strings. If None, use sys.argv
        :return:
        """
        args = self.parse(cli_args=cli_args)
        get_input_args.prepare_with_side_effects(args.out_dir, args.log_dir,
                                                 args.cache_dir, args.verbose,
                                                 args.resume)
        if not args.resume:
            get_input_args.write_args_to_file(args)
        return args


def main(cli_args=None):
    """
    Args:
        cli_args: If none then use sys.argv (the usual pattern except when
            testing)
    """
    da = DalynatorApplication()
    args = da.parse_and_initialize(cli_args=cli_args)

    swarm = djs.DalynatorJobSwarm(
        tool_name=args.tool_name,

        input_data_root=args.input_data_root,
        out_dir=args.out_dir,
        codcorrect_version=args.codcorrect_version,
        fauxcorrect_version=args.fauxcorrect_version,
        epi_version=args.epi_version,
        paf_version=None,
        cause_set_ids=None,
        output_version=args.output_version,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,

        years=args.years,
        n_draws=args.n_draws,
        mixed_draw_years=args.mixed_draw_years,

        start_year_ids=args.start_year_ids,
        end_year_ids=args.end_year_ids,

        location_set_ids=args.location_set_ids,
        measures=args.measures,

        write_out_star_ids=False,
        skip_cause_agg=False,

        start_at=args.start_at,
        end_at=args.end_at,
        upload_to_test=args.upload_to_test,
        read_from_prod=args.read_from_prod,
        dual_upload=args.dual_upload,

        turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,

        cache_dir=args.cache_dir,

        sge_project=args.sge_project,
        verbose=args.verbose,
        raise_on_paf_error=False,
        do_not_execute=args.do_not_execute
    )
    swarm.run("run_pipeline_dalynator.py")
    return swarm


if __name__ == "__main__":
    main()

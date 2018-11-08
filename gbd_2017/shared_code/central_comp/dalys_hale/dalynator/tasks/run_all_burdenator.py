from dalynator import DalynatorJobSwarm as djs
from dalynator.tasks import run_all_dalynator as rad


class BurdenatorApplication(rad.DalynatorApplication):
    """
    Most methods are identical, just the parsing is a bit different.
    """

    def _add_non_resume_args_to_parser(self):
        # NOTE: This extends DalynatorApplication's parser
        rad.DalynatorApplication._add_non_resume_args_to_parser(self)
        # Now add the Burdenator specific args
        self.parser.add_argument('-p', '--paf_version', required=True,
                                 type=int, action='store',
                                 help='The version of the paf results to use, '
                                      'an integer')

        self.parser.add_argument('--cause_set_id',
                                 default=2,
                                 type=int, action='store',
                                 help='The cause_set_id to use for the '
                                      'cause_hierarchy, an integer')

        self.parser.add_argument('--daly',
                                 type=int, action='store',
                                 dest='daly_version',
                                 help='The version of the dalynator results '
                                      'to use, an integer')

        self.parser.add_argument('--ylls_paf',
                                 action='store_true', default=False,
                                 help='write_out_ylls_paf',
                                 dest="write_out_ylls_paf")

        self.parser.add_argument('--ylds_paf',
                                 action='store_true', default=False,
                                 help='Write out the back-calculated pafs for '
                                      'ylds',
                                 dest="write_out_ylds_paf")

        self.parser.add_argument('--deaths_paf',
                                 action='store_true', default=False,
                                 help='Write out the back-calculated pafs for '
                                      'deaths',
                                 dest="write_out_deaths_paf")

        self.parser.add_argument('--dalys_paf',
                                 action='store_true', default=False,
                                 help='Write out the back-calculated pafs for '
                                      'dalys',
                                 dest="write_out_dalys_paf")

        self.parser.add_argument('--star_ids',
                                 action='store_true', default=False,
                                 help='Write out the star_id column',
                                 dest="write_out_star_ids")
        return self.parser

    def parse(self, tool_name="burdenator", cli_args=None):
        args = rad.DalynatorApplication.parse(self, tool_name=tool_name,
                                              cli_args=cli_args)

        if not any([args.write_out_ylls_paf, args.write_out_ylds_paf,
                    args.write_out_deaths_paf]):
            raise ValueError("must choose at least one of --ylls_paf, "
                             "--ylds_paf and --deaths_paf")

        if args.write_out_ylds_paf:
            if not args.epi_version:
                raise ValueError("An epi_version is needed if yld's are being "
                                 "burdenated, i.e. if --yld_pafs is set")

        if not args.location_set_id:
            raise ValueError("To aggregate risk-attributable burden "
                             "up the location hierarchy, either "
                             "--location_set_id or --location_set_version_id "
                             "must be provided")
        return args

    def parse_and_initialize(self, cli_args=None):
        """
        Convenience for testing
        Args:
            args_strings:  None or a list of strings. If None then read use
                sys.argv

        Returns:
            args from argparse
        """
        args = self.parse(cli_args=cli_args, tool_name='burdenator')
        self.prepare_with_side_effects(args.out_dir, args.log_dir,
                                       args.cache_dir, args.verbose,
                                       args.resume)
        if not args.resume:
            self.write_args_to_file(args)
        return args


def main(cli_args=None):
    """
    Args:
        cli_args: If none then use sys.argv (the usual pattern except when
            testing)
    """
    ba = BurdenatorApplication()
    args = ba.parse_and_initialize(cli_args=cli_args)

    swarm = djs.DalynatorJobSwarm(
        tool_name=args.tool_name,

        input_data_root=args.input_data_root,
        out_dir=args.out_dir,
        cod_version=args.cod_version,
        epi_version=args.epi_version,
        paf_version=args.paf_version,
        daly_version=args.daly_version,
        version=args.version,

        cause_set_id=args.cause_set_id,
        gbd_round_id=args.gbd_round_id,

        year_ids_1=args.year_ids_1,
        n_draws_1=args.n_draws_1,
        year_ids_2=args.year_ids_2,
        n_draws_2=args.n_draws_2,

        start_year_ids=args.start_year_ids,
        end_year_ids=args.end_year_ids,

        location_set_version_id=args.location_set_version_id,

        add_agg_loc_set_ids=args.add_agg_loc_set_ids,

        no_sex_aggr=args.no_sex_aggr,
        no_age_aggr=args.no_age_aggr,
        write_out_ylds_paf=args.write_out_ylds_paf,
        write_out_ylls_paf=args.write_out_ylls_paf,
        write_out_deaths_paf=args.write_out_deaths_paf,
        write_out_dalys_paf=args.write_out_dalys_paf,
        write_out_star_ids=args.write_out_star_ids,

        start_at=args.start_at,
        end_at=args.end_at,
        upload_to_test=args.upload_to_test,

        turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,

        cache_dir=args.cache_dir,

        sge_project=args.sge_project,
        verbose=args.verbose,
        raise_on_paf_error=args.raise_on_paf_error,
        do_not_execute=args.do_not_execute
    )
    swarm.run("run_pipeline_burdenator.py")
    return swarm


if __name__ == "__main__":
    main()

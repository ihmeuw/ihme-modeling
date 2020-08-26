import logging
import os

import pandas as pd

from dalynator import get_input_args
from dalynator.constants import UMASK_PERMISSIONS
from dalynator.write_csv import sub_pub_for_cc


os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)

PK_SINGLE_YEAR_RISK = ['location_id', 'cause_id', 'rei_id', 'age_group_id',
                       'year_id', 'metric_id', 'measure_id', 'sex_id']
PK_MULTI_YEAR_RISK = ['location_id', 'cause_id', 'rei_id', 'age_group_id',
                      'year_start_id', 'year_end_id', 'metric_id',
                      'measure_id', 'sex_id']
PK_SINGLE_YEAR_NORISK = ['location_id', 'cause_id', 'age_group_id', 'year_id',
                         'metric_id', 'measure_id', 'sex_id']
PK_MULTI_YEAR_NORISK = ['location_id', 'cause_id', 'age_group_id',
                        'year_start_id', 'year_end_id', 'metric_id',
                        'measure_id', 'sex_id']


class ColumnstoreSorter(object):

    def __init__(self, args, table_class, n_years):
        """See the parser+arg constructors for details about the expected
        args. table_class is expected to be in the set {eti, risk, summary}.
        n_years should be in the set {single_year, multi_year}"""

        if table_class not in ["eti", "risk", "summary"]:
            raise ValueError("table_class is expected to be in the "
                             "set {eti, risk, summary}")
        if n_years not in ["single_year", "multi_year"]:
            raise ValueError("n_years is expected to be in the "
                             "set {single_year, multi_year}")

        self.location_id = args.location_id

        if n_years == "single_year":
            self.year_postfixes = args.year_ids
        elif n_years == "multi_year":
            self.year_postfixes = [
                "FILEPATH".format(s, e)
                for s, e in zip(args.start_year_ids, args.end_year_ids)]
        self.measure_ids = args.measure_ids
        self.tool_name = args.tool_name

        self.n_years = n_years
        self.file_prefix = "FILEPATH".format(table_class)

        self.root_dir = sub_pub_for_cc(
            os.path.join(args.out_dir, "draws", str(self.location_id)))

        self.outfile_dir = os.path.join(self.root_dir, "upload")
        self.outfile_basename = "FILEPATH".format(
            loc=self.location_id, tc=table_class, ny=self.n_years)

    def _get_csv_list(self):
        file_paths = []
        for year_pf in self.year_postfixes:
            for meas in self.measure_ids:
                fd = os.path.join(self.root_dir, "upload", str(meas),
                                  self.n_years)
                fp = "FILEPATH".format(
                    prefix=self.file_prefix, loc=self.location_id, yr=year_pf)
                file_paths.append(os.path.join(fd, fp))
        return file_paths

    def _read_csvs_to_dataframe(self, csv_list):
        df = pd.concat([pd.read_csv(f) for f in csv_list])
        return df

    def _sort_frame(self, df):
        if self.n_years == "single_year":
            if self.tool_name == "burdenator":
                sort_order = PK_SINGLE_YEAR_RISK
            elif self.tool_name == "dalynator":
                sort_order = PK_SINGLE_YEAR_NORISK
        elif self.n_years == "multi_year":
            if self.tool_name == "burdenator":
                sort_order = PK_MULTI_YEAR_RISK
            elif self.tool_name == "dalynator":
                sort_order = PK_MULTI_YEAR_NORISK
        return df.sort_values(sort_order)

    def reduce_to_meas_csvs(self):
        csvs = self._get_csv_list()
        df = self._read_csvs_to_dataframe(csvs)
        sorted_df = self._sort_frame(df)
        for measure_id in sorted_df.measure_id.unique():
            filepath = "FILEPATH".format(od=self.outfile_dir,
                                              m=measure_id,
                                              ob=self.outfile_basename)
            write_df = sorted_df.query("measure_id == {}".format(measure_id))
            write_df.to_csv(filepath, index=False)


def main(cli_args=None):
    parser = get_input_args.construct_parser_cs_sort()
    args = get_input_args.construct_args_cs_sort(parser, cli_args=cli_args)

    # Handle different file permutations based on tool
    if args.tool_name == "burdenator":
        sorter = ColumnstoreSorter(args, "risk", "single_year")
        sorter.reduce_to_meas_csvs()

        sorter = ColumnstoreSorter(args, "eti", "single_year")
        sorter.reduce_to_meas_csvs()

        if args.start_year_ids is not None:
            sorter = ColumnstoreSorter(args, "risk", "multi_year")
            sorter.reduce_to_meas_csvs()

            sorter = ColumnstoreSorter(args, "eti", "multi_year")
            sorter.reduce_to_meas_csvs()

    elif args.tool_name == "dalynator":
        sorter = ColumnstoreSorter(args, "summary", "single_year")
        sorter.reduce_to_meas_csvs()

        if args.start_year_ids is not None:
            sorter = ColumnstoreSorter(args, "summary", "multi_year")
            sorter.reduce_to_meas_csvs()

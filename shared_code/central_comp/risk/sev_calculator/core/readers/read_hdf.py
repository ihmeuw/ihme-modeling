# Command line interface for extracting data from hdf files
from argparse import RawTextHelpFormatter
import pandas as pd
import argparse
import sys
from functools import partial
import concurrent.futures as cf


def parse_args():
    '''
    Parse all the args...
    '''
    parser = argparse.ArgumentParser(
        description='''
        Reads HDF files and write to disk or stdout.

        Example Usage:
        python read_hdf.py ./path/to/foo.h5
                    --where "age_group_id == 27 and sex_id in [1,2]"
                    --columns location_id draw_1

        For more help, type python read_hdf.py -h
        ''', formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_file', type=str, nargs='*', help='''path to h5
                        file to read. Can accept multiple files ''')
    parser.add_argument('--columns', type=str, nargs='*',
                        help=('optional argument to specify a subset of columns'
                              ' to return'))
    parser.add_argument('--where', type=str, nargs='?',
                        help='''optional argument that specifies conditions
                              which must be true for the row to be included.
                              IE, "sex_id in [1,2] and age_group_id != 27 and
                              cause_id == 294". See
                              http://pandas.pydata.org/pandas-docs/stable/
                              io.html#io-hdf5''')
    parser.add_argument('--out_file', type=str, nargs='?',
                        help='''Optional. If specified, will write data to that
                        path. If unspecified, will write to stdout''',
                        dUSERt=sys.stdout)
    parser.add_argument('--file_format', type=str, nargs='?', dUSERt='csv',
                        help='''Optional. If specified, will write data as that
                        file_format. If unspecified, will write as csv.
                        Allowable types are: {dct, csv, dta}. Note that dta
                        can't be streamed, and stata can't read a csv stream
                        because import delimited attemptes random I/O. If you
                        want to stream to stata, use read_hdf.ado''')
    parser.add_argument('--num_slots', type=int, dUSERt=1,
                        help='''Optionally specify how many slots/cores to spawn
                        while reading hdf files. So if you're calling this from
                        stata-mp (which uses 4), and your job is a 10 slot job,
                        this can be up to 5''')
    args = parser.parse_args()

    if not args.out_file:
        args.out_file = sys.stdout

    return args


def write_header(df, fname):
    '''write a stata infile dictionary header'''

    raw_dtypes = ((col, dt) for (col, dt) in
                  zip(df.columns, df.dtypes.astype(str)))
    stata_types = map(convert_dtype, raw_dtypes)

    if fname is sys.stdout:
        fname.write('infile dictionary { \n')
        for (stata_type, col) in zip(stata_types, df.columns):
                fname.write(stata_type + ' ' + col + '\n')
        fname.write('}\n')
    else:
        with open(fname, 'w') as f:
            f.write('infile dictionary { \n')
            for (stata_type, col) in zip(stata_types, df.columns):
                    f.write(stata_type + ' ' + col + '\n')
            f.write('}\n')


def convert_dtype((col, dtype)):
    if 'int' in dtype:
        # location_ids exceed 32,740, so can't return int.
        # Stata reader should compress after it infiles
        return 'long'
    if 'float' in dtype:
        return 'double'
    else:
        # if we're reading more than one dataset, there's no gaurentee
        # that the first dataset has the maximum string length.
        # So it's possible this will truncate strings
        return 'str' + str(df[col].str.len().max())


def to_dct(df, fname, include_header):
    ''' take a pandas dataframe and write a stata .dct file '''
    if include_header:
        write_header(df, fname)
    df.to_csv(fname, mode='a', index=False, header=False, sep=" ")


def get_write_func(f_fmt, args, file_num):
    '''
    Depending on the file format, figure out what function to use to
    export the data. if this is first file being output, include header
    '''

    if args.out_file is sys.stdout and file_num == 0:
        include_header = True
    else:
        # this isn't first df in stream, don't include header
        include_header = False

    # return a curried function depending on file type
    func_d = {'dta': partial(pd.DataFrame.to_stata, fname=args.out_file),
              'csv': partial(pd.DataFrame.to_csv, path_or_buf=args.out_file,
                             index=False, header=include_header),
              'dct': partial(to_dct, fname=args.out_file,
                             include_header=include_header)}

    return func_d[f_fmt]


def file_reader(jobs):
    # for each file in the list of input files, read them in split amongst
    # args.num_slots workers
    with cf.ProcessPoolExecutor(max_workers=args.num_slots) as e:
        futures = []
        for f in args.input_file:
            futures.append(e.submit(pd.read_hdf, f, key="draws",where=args.where))

        while futures:
            done, futures = cf.wait(futures, return_when='FIRST_COMPLETED')
            for h5 in done:
                yield h5.result()

if __name__ == '__main__':

    args = parse_args()
    assert args.out_file is sys.stdout, 'only supporting streams, right now'
    assert args.file_format == 'dct', 'only support dct, right now'

    # as the files are done reading, iterate through them and pipe/output
    # them.  Use enumerate to keep track of the first file, which is the
    # only one with a header
    num_files = 0
    for (i, h5) in enumerate(file_reader(args.input_file)):
        # if index is meaningful, reset it
        if not h5.index.is_integer():
            h5 = h5.reset_index()
        write_func = get_write_func(args.file_format, args, i)
        write_func(h5)
        num_files = num_files + 1

    # if we didn't write out any files, spit out an error message to stdout
    # so stata doesn't hang
    if num_files == 0:
        print("Didn't find any files")

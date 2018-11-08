
import argparse
import numpy as np
import os
import pandas as pd
import sys
from os.path import join
from time import sleep

def compile_save_vr(in_directory,out_filepath, encoding):
    # Read in all files within the input directory and concatenate to a single
    #  dataframe
    all_files = [join(in_directory,f) for f in os.listdir(in_directory)
                    if os.path.isfile(join(in_directory,f))]
    compiled = pd.concat([pd.read_csv(fp,encoding=encoding)
                           for fp in all_files])
    compiled.to_csv(out_filepath,index=False,encoding=encoding)
    print("Compiled VR file saved to {}".format(out_filepath))
    return None


def remove_directory(thisdir):
    # Delete all the temporary files stored in the directory
    for f in os.listdir(thisdir):
        os.remove(join(thisdir,f))
    # Delete the temporary directory
    os.rmdir(thisdir)
    print("Temporary VR data deleted from {}; folder removed.".format(thisdir))
    return None


if __name__=="__main__":
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--indir",type=str,
                        help="The input directory, which should contain ONLY "
                             "pulled VR files")
    parser.add_argument("-o","--outfile",type=str,
                        help="The CSV file where pulled VR data will be saved")
    parser.add_argument("-n","--encoding",type=str,
                        help="Encoding for the final saved file")
    cmd_args = parser.parse_args()
    assert cmd_args.indir is not None, "Program requires an input directory"
    assert cmd_args.outfile is not None, "Program requires an output filepath"
    assert cmd_args.outfile not in cmd_args.indir, ("The output filepath should"
            " NOT be stored within the input directory, as it will be deleted.")
    # Compile the VR data into a single file
    compile_save_vr(in_directory=cmd_args.indir,
                    out_filepath=cmd_args.outfile,
                    encoding=cmd_args.encoding)
    # Test 5 times that the file has been created and is recognized by the system
    num_tries = 0
    while (num_tries < 5 and not os.path.exists(cmd_args.outfile)):
        num_tries = num_tries + 1
        sleep(2)

    # Remove all temporary VR files from the folder
    remove_directory(cmd_args.indir)
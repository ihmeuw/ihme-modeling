
import re
import glob
import os


def modify_sas_scripts(years, request_id):
    """
    Prep the sas scripts for CMS so they run with 1 click
    """

    for year in years:
        pdir = "FILEPATH"

        script_dir = "FILEPATH"
        mod_files = glob.glob("FILEPATH")
        if len(mod_files) > 0:
            print("removing {} modified SAS scripts already present".format(len(mod_files)))
            [os.remove(f) for f in mod_files]

        files = glob.glob("FILEPATH")

        for f in files:
            # Read in the file
            with open(f, 'r') as file :
              filedata = file.read()

            data_name = os.path.basename(f)[:-12]

            # get the filename from line 28 and remove it
            m = re.search('filename in.+', filedata)
            fname28 = m.group(0)

            # get the input dat filename
            dat_fname = re.search('/' + data_name + '.+.dat', fname28).group(0)
            # create the csv name
            csv_fname = dat_fname[:-4] + ".csv"

            # replace the filepath with nothing
            fname28_replace = fname28.replace(dat_fname, "")

            # and now replace it in the full SAS script
            filedata = filedata.replace(fname28, fname28_replace)

            # replace line 19 with the parent dir + filename
            o19 = re.search("%let.+", filedata).group(0)
            r19 = pdir + dat_fname
            r19 = o19.replace(".", "'" + r19 + "'")
            # replace it in the full script
            filedata = filedata.replace(o19, r19)

            o25 = "FILEPATH" 
            r25 = "FILEPATH" 
            filedata = filedata.replace(o25, r25)

            # get the data=`` value
            d_line = re.search("data=ccw.+ ", filedata).group(0)[:-1]
            out_fname = "FILEPATH"

            new_end =\
            """PROC EXPORT {d}
    outfile="{out}" dbms=csv replace;
run;
* End of Program *;
            """.format(d=d_line, out=out_fname)

            # datasets with multiple .dat files have a * in the filepath
            # Remove that when writing a CSV
            new_end = new_end.replace("*.csv", ".csv")
            eop = re.search(".+End of Program.+", filedata).group(0)

            filedata = filedata.replace(eop, new_end)

            # write back to drive
            newf = "FILEPATH"
            with open(newf, 'w') as file:
              file.write(filedata)
        print("finished modifying {} files for year {}".format(len(files), year))
    return "done with all years"


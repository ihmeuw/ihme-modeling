# era5_download
This folder contains the code used to download the ERA5 temperature data from this website: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form

The code is run as an array job using the following terminal command (with the usual modification for use by others):
sbatch -J era5_download --array=0-504 --mem=20G -c 1 -e /ihme/temp/slurmoutput/mjassmus/errors/era5_download_%A_%a.err -o /ihme/temp/slurmoutput/mjassmus/output/era5_download_%A_%a.out -t 24:00:00 -A proj_erf -p all.q launcher.sh

The launcher will launch the download code, which will save the data files in its current working directory. As of 10/31/2022, the download script is hardcoded to switch its working directory to "/ihme/erf/ERA5/michael_download_20221031/". This can be adjusted in line 13 of the script as needed.
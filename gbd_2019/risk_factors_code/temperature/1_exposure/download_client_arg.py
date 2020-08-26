import sys
sys.path.append('FILEPATH')
import cdsapi
import os
import subprocess

year_month = sys.argv[1]

def download(year_month):	
	[year, month] = year_month.split("_")

	c = cdsapi.Client()	
	c.retrieve(
		'reanalysis-era5-single-levels',
		{
			'product_type':'reanalysis',
			'format':'netcdf',
			'variable':['2m_dewpoint_temperature','2m_temperature'],
			'year':f'{year}',
			'month':f'{month}',
			'day':['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'],
			'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00','09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00','18:00','19:00','20:00','21:00','22:00','23:00']
		},		
		f'FILEPATH')
		
if __name__ == "__main__":
    download(year_month)
    
    multi_slot = '-pe multi_slot 12'
    error = f'FILEPATH'
    output = 'FILEPATH'
    python = 'FILEPATH'
    clean_client = 'FILEPATH'
    dl_dir = 'FILEPATH'
    
    qsub = f'qsub -b y -N cl{month} {multi_slot} {error} {output} {python} {clean_client} {year_month}'
    subprocess.call(qsub,shell=True)
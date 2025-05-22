source FILEPATH activate v1
pip install db_tools

ipython

cd FILEPATH

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders import add_nid_metadata
from cod_prep.downloaders import get_map_version
from cod_prep.downloaders import add_code_metadata

iso3 = ISO # put iso code of the country for which you want to extract the VR data

df = get_claude_data(phase='disaggregation', iso3=iso3, data_type_id=9, refresh_id=27, is_active=True, force_rerun=True, block_rerun=False)
df = add_nid_metadata(df, 'code_system_id')
df.to_csv(path_or_buf='FILEPATH' + iso3 + '_split_noCodes.csv', index=False)

df = get_claude_data(phase='redistribution', iso3=iso3, data_type_id=9, refresh_id=27, is_active=True, force_rerun=True, block_rerun=False)
df.to_csv(path_or_buf='FILEPATH' + iso3 + '_rd.csv', index=False)

df = get_claude_data(phase='misdiagnosiscorrection', iso3=iso3, data_type_id=9, refresh_id=27, is_active=True, force_rerun=True, block_rerun=False)
df.to_csv(path_or_buf='FILEPAHT' + iso3 + '_corr.csv', index=False)
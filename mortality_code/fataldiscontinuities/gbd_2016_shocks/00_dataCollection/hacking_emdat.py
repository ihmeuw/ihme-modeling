import pandas as pd
import requests
import string
import platform


# multi-platform paths -- apprently python and globals don't mix?
if 'Windows' in platform.platform():
    prefix = 'PREFIX'
else:
    prefix = 'PREFIX'

def get_emdat_data(disaster_type, continent):
    url = "http://www.emdat.be/disaster_list/php/search.php?_dc=1446683909838&continent={continent}&region=&iso=&from=1900&to=2015&subgroup=&type={disaster_type}&options=associated_dis%2Cassociated_dis2%2Ctotal_deaths%2Ctotal_affected%2Ctotal_dam%2Cinsur_dam&page=1&start=0&limit=25".format(continent=continent, disaster_type=disaster_type)
    r = requests.get(url)
    return pd.DataFrame(r.json()['data'])

def ascii_convert(s):
    try:
        return filter(lambda x: x in string.printable, s)
    except:
        return s
		
# Get Disaster Types
url = "http://www.emdat.be/disaster_list/php/listDisasterTypes.php?_dc=1446683736578&page=1&start=0&limit=25"
r = requests.get(url)
disaster_types_data = pd.DataFrame(r.json()['data'])

print('downloading data:')
output = []
for continent in ['Africa', 'Americas', 'Asia', 'Europe', 'Oceania']:
    for disaster_type in disaster_types_data['dis_type'].drop_duplicates():
        print(continent, disaster_type)
        temp = get_emdat_data(disaster_type, continent)
        temp['continent_orig'] = continent
        temp['disaster_type_orig'] = disaster_type
        output.append(temp)
output = pd.concat(output)


print('converting columns to ascii format:')
t = output.reset_index(drop=True)
for c in t.columns:
    t[c] = t[c].map(lambda x: ascii_convert(x))
    print(c)

#t.to_csv('FILEPATH')
t.to_csv(prefix + 'FILEPATH') # AT, for GBD 2016
print("done with EM-DAT webscrape!")

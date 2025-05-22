from geolocation_open_street_maps import *
from gen_gps_matched_locations import *
from urllib import parse
from ast import literal_eval
import time
import json


def format_osm_location_args(country, admin1, admin2, admin3, urban_rural):
    if urban_rural == 1:
        urbal = "urban, "
    elif urban_rural == 0:
        urbal = "rural, "
    else:
        urbal = ""
    location_string = urbal + str(admin3) + ", " + str(admin2) + ", " + str(admin1) + ", " + country
    location_string = location_string.strip(",")
    request_dict = {'q': location_string}
    q = parse.urlencode(request_dict)
    return q

def generate_geolocated_matched_file(df, source, version):
    cols = ['source_event_id', 'country', 'admin1', 'admin2',
            'admin3', 'urban_rural', 'side_a', 'side_b']
    df = df[cols]
    df = df.fillna("")
    df['side_a_string'] = df['side_a'].copy()
    df['side_b_string'] = df['side_b'].copy()
    df['location_id'] = float("nan")
    df['osm_query'] = df.apply(lambda x: format_osm_location_args(country=x['country'],
                                                                  admin1=x['admin1'],
                                                                  admin2=x['admin2'],
                                                                  admin3=x['admin3'],
                                                                  urban_rural=x['urban_rural']),
                               axis=1)
    event_count = df.copy().shape[0] + 1
    sets_of_50 = int(event_count / 200) + 1
    final = pd.DataFrame()
    print("chunks completed out of {}".format(sets_of_50))
    event_chunk = 0
    resume = 0
    event_chunk = resume
    for event_chunk in range(resume, sets_of_50):
        print(event_chunk)
        index = event_chunk * 200
        start = index
        end = index + 200
        results = df.copy()[start:end]
        try:
            results['osm_json_results'] = results['osm_query'].apply(
                lambda x: json.loads(osm_query(x)))
        except Exception as e:
            print(e, event_chunk)

        results.to_csv(FILEPATH.format(source=source, x=event_chunk))

        final = final.append(results)

    df = final
    df['osm_num_results'], df['osm_centroid_lat'], \
        df['osm_centroid_long'], df['osm_buffer_ns_km'], df['osm_buffer_ew_km'], \
        df['osm_r1_address'], df['osm_r1_type'], df['osm_r1_lat'], df['osm_r1_long'], \
        df['osm_r1_buffer_ns'], df['osm_r1_buffer_ew'], \
        df['osm_r2_address'], df['osm_r2_type'], df['osm_r2_lat'], \
        df['osm_r2_long'], df['osm_r2_buffer_ns'], \
        df['osm_r2_buffer_ew'] = zip(*df['osm_json_results'].map(parse_osm_gm_result))
    df = df.rename(columns={"osm_r1_lat": "latitude",
                            "osm_r1_long": "longitude"})
    df['side_a'] = float("nan")
    df['side_b'] = float("nan")
    df = df[cols + ["latitude", "longitude"]]
    if df.shape[0] >= 1:
        generate_gps_matched_file(df,
                                  source,
                                  filename="{}_open_street_maps_location_map.csv",
                                  version=version,
                                  artificial_coords=True
                                  )

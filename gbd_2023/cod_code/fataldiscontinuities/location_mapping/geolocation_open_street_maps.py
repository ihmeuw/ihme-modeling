import numpy as np
import urllib
from time import sleep


class SingleResult:
    def __init__(self, result_dict, source):
        if source not in ["OSM", "Google Maps"]:
            raise ValueError("Source should be one of 'OSM' or 'Google Maps'")
        if source == "Google Maps":
            geom = result_dict['geometry']
            self.centroid = SingleLocation(geom['location'])
            if "bounds" in geom:
                self.northeast = SingleLocation(geom['bounds']['northeast'])
                self.southwest = SingleLocation(geom['bounds']['southwest'])
            else:
                self.northeast = self.centroid
                self.southwest = self.centroid
            self.address = result_dict['formatted_address']
            if "partial_match" in result_dict:
                self.partial_match = "Yes"
            else:
                self.partial_match = "No"
            self.place_type = "Unknown"
        else:
            obj = result_dict
            self.centroid = SingleLocation({'lat': obj['lat'],
                                            'lng': obj['lon']})
            if "boundingbox" in obj:
                self.northeast = SingleLocation({'lat': obj['boundingbox'][1],
                                                 'lng': obj['boundingbox'][3]})
                self.southwest = SingleLocation({'lat': obj['boundingbox'][0],
                                                 'lng': obj['boundingbox'][2]})
            else:
                self.northeast = self.centroid
                self.northwest = self.centroid

            if "display_name" in obj:
                self.address = obj['display_name']
            else:
                self.address = "Unknown"

            if "type" in obj:
                self.place_type = obj['type']
            self.partial_match = "Unknown"
        self.buffer_ns_km, self.buffer_ew_km = get_uncertainty(self.northeast,
                                                               self.southwest)


class SingleLocation:
    def __init__(self, location_dict):
        self.lat = float(location_dict['lat'])
        self.long = float(location_dict['lng'])

def get_uncertainty(northeast, southwest):
    if ((np.absolute(northeast.lat) > 90) or (np.absolute(southwest.lat) > 90)):
        raise ValueError("Lats should be between -90 and 90 decimal degrees.")
    if ((np.absolute(northeast.long) > 180) or (np.absolute(southwest.long) > 180)):
        raise ValueError("Longs should be between -180 and 180 decimal degrees.")

    centroid = get_centroid(northeast, southwest)
    km_per_dd_northsouth = 111.32
    centroid_lat_radians = np.deg2rad(centroid.lat)
    km_per_dd_eastwest = 111.32 * np.cos(centroid_lat_radians)
    delta_ns_dd = northeast.lat - southwest.lat
    delta_ns_km = delta_ns_dd * km_per_dd_northsouth
    delta_ew_dd = northeast.long - southwest.long
    delta_ew_km = delta_ew_dd * km_per_dd_eastwest
    return delta_ns_km, delta_ew_km

def get_centroid(northeast, southwest):
    center_lat = np.mean([northeast.lat, southwest.lat])
    center_long = np.mean([northeast.long, southwest.long])
    center_dict = {'lat': center_lat,
                   'lng': center_long}
    centroid = SingleLocation(center_dict)
    return centroid

def format_osm_args(address_text, country):
    request_dict = {'q': str(address_text) + ", " + country}
    q = urllib.parse.urlencode(request_dict)
    return q


def osm_query(url_args, output_type="json"):
    if output_type not in ['html', 'xml', 'json']:
        raise ValueError("output_type must be one of 'html', 'xml', or 'json'")
    base_url = ADDRESS
    combined_url = "{}{}&format={}".format(base_url, url_args, output_type)
    try:
        with urllib.request.urlopen(combined_url, timeout=30) as response:
            raw_output = response.read().decode('utf-8')
    except urllib.error.HTTPError:
        raw_output = '[]'
    except Exception as e:
        print("there was an error!", e)
        raw_output = '[]'
        sleep(1)
    sleep(.1)
    return raw_output


def parse_osm_gm_result(full_json):
    result_source = ''
    status = ''
    bounded_ns_km = np.nan
    bounded_ew_km = np.nan
    bounding_centroid_lat = np.nan
    bounding_centroid_long = np.nan
    r1_address = ''
    r1_type = ''
    r1_lat = np.nan
    r1_long = np.nan
    r1_buffer_ns = np.nan
    r1_buffer_ew = np.nan
    r1_partial_match = ''
    r2_address = ''
    r2_type = ''
    r2_lat = np.nan
    r2_long = np.nan
    r2_buffer_ns = np.nan
    r2_buffer_ew = np.nan
    r2_partial_match = ''

    if type(full_json) == dict:
        result_source = "Google Maps"
        status = full_json['status']
    elif type(full_json) == list:
        result_source = "OSM"
        if len(full_json) > 0:
            status = "OK"
    else:
        raise ValueError("The full_json object should be in dict or list format")

    num_results = 0
    results_list = []
    if status == "OK":
        if result_source == "Google Maps":
            full_json = full_json['results']
        num_results = len(full_json)
        for result_json in full_json:
            this_result = SingleResult(result_json, source=result_source)
            results_list.append(this_result)
        far_north = max([i.northeast.lat for i in results_list])
        far_south = min([i.southwest.lat for i in results_list])
        far_east = max([i.northeast.long for i in results_list])
        far_west = min([i.southwest.long for i in results_list])
        bounded_all_northeast = SingleLocation({'lat': far_north,
                                                'lng': far_east})
        bounded_all_southwest = SingleLocation({'lat': far_south,
                                                'lng': far_west})
        bounded_ns_km, bounded_ew_km = get_uncertainty(bounded_all_northeast,
                                                       bounded_all_southwest)
        bounding_centroid_lat = np.mean([far_north, far_south])
        bounding_centroid_long = np.mean([far_east, far_west])
        if num_results >= 1:
            r1_address = results_list[0].address
            r1_type = results_list[0].place_type
            r1_lat = results_list[0].centroid.lat
            r1_long = results_list[0].centroid.long
            r1_buffer_ns = results_list[0].buffer_ns_km
            r1_buffer_ew = results_list[0].buffer_ew_km
            r1_partial_match = results_list[0].partial_match
        if num_results >= 2:
            r2_address = results_list[1].address
            r2_type = results_list[1].place_type
            r2_lat = results_list[1].centroid.lat
            r2_long = results_list[1].centroid.long
            r2_buffer_ns = results_list[1].buffer_ns_km
            r2_buffer_ew = results_list[1].buffer_ew_km
            r2_partial_match = results_list[1].partial_match
    if result_source == "Google Maps":
        return (status, num_results,
                bounding_centroid_lat, bounding_centroid_long,
                bounded_ns_km, bounded_ew_km,
                r1_address, r1_lat, r1_long,
                r1_buffer_ns, r1_buffer_ew, r1_partial_match,
                r2_address, r2_lat, r2_long,
                r2_buffer_ns, r2_buffer_ew, r2_partial_match)
    else:
        return (num_results, bounding_centroid_lat, bounding_centroid_long,
                bounded_ns_km, bounded_ew_km,
                r1_address, r1_type, r1_lat, r1_long, r1_buffer_ns, r1_buffer_ew,
                r2_address, r2_type, r2_lat, r2_long, r2_buffer_ns, r2_buffer_ew)

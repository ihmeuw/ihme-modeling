import pandas as pd
import numpy as np
import shapely as sly
import geopandas as gpd
from db_queries import get_location_metadata
from datetime import datetime as dt
import os

LOCATION_FOLDER = FILEPATH
LOCATION_INPUT_FOLDER = os.path.join(LOCATION_FOLDER, "inputs")
LOCATION_OUTPUT_FOLDER = os.path.join(LOCATION_FOLDER, "outputs")


def df_to_points_gdf(in_data, lat_col='latitude', lon_col='longitude',
                     drop_lat_lon=False):
    assert type(in_data) is pd.core.frame.DataFrame, "'in_data' was not a DataFrame"
    assert np.all([i in in_data.columns for i in [lat_col, lon_col]]), "Missing lat/long columns"
    assert "geometry" not in in_data.columns, (
        "'geometry' cannot already be a column in the dataframe - this will be overwritten")
    geometry = [sly.geometry.Point(xy) for xy in zip(in_data[lon_col], in_data[lat_col])]
    if drop_lat_lon:
        in_data.drop(labels=[lon_col, lat_col], axis=1, inplace=True)
    points_df = gpd.GeoDataFrame(in_data,
                                 crs={'init': 'epsg:4326'},
                                 geometry=geometry)
    return points_df


def load_master_shapefile(location_set_id=21, gbd_round_id=6,
                          shp_filepath=(FILEPATH)):
    shp = gpd.read_file(shp_filepath)
    shp.rename(columns={"loc_id": "location_id"}, inplace=True)
    meta = get_location_metadata(location_set_id=location_set_id,
                                 gbd_round_id=gbd_round_id)
    merge_cols = ['location_id', 'path_to_top_parent', 'most_detailed']
    shp_joined = shp.merge(meta.loc[:, merge_cols],
                           on=["location_id"], how='inner')
    dropped_rows = shp.shape[0] - shp_joined.shape[0]
    if dropped_rows > 0:
        missing_rows = [i for i in shp['location_id'].unique().tolist()
                        if i not in shp_joined['location_id'].unique().tolist()]
        print("{} locations were dropped from the master "
              "shapefile due to the merge: {}".format(dropped_rows, missing_rows))
    shp_joined = shp_joined.loc[shp_joined['most_detailed'] == 1, :]
    dropped_rows = shp.shape[0] - shp_joined.shape[0]
    if dropped_rows > 0:
        print("{} locations were dropped because they were not most detailed".format(dropped_rows))
    shp_joined = shp_joined.drop(labels='most_detailed', axis=1)
    return shp_joined


def construct_descendants_dict(loc_meta):
    not_most_detailed = loc_meta.loc[loc_meta['most_detailed'] == 0,
                                     'location_id'].tolist()
    detailed_df = loc_meta.loc[loc_meta['most_detailed'] == 1, :]
    descendents = dict()
    not_most_detailed = [i for i in not_most_detailed if i != 1]
    descendents[1] = detailed_df['location_id'].tolist()
    for parent_id in not_most_detailed:
        parent_text = ",{},".format(parent_id)
        descendents[parent_id] = detailed_df.loc[
            detailed_df['path_to_top_parent'].apply(
                lambda x: parent_text in x), 'location_id'].tolist()
    for detailed_id in detailed_df['location_id'].tolist():
        descendents[detailed_id] = [detailed_id]
    return descendents


def overlay_polygons(points_df, polys_df, polys_cols_to_join):
    if type(polys_cols_to_join) is not list:
        polys_cols_to_join = [polys_cols_to_join]
    assert np.all([i in polys_df.columns for i in polys_cols_to_join]), (
        "Not all items in {} are column names in the polys_df".format(polys_cols_to_join))
    polys_df = polys_df.loc[:, ['geometry'] + polys_cols_to_join]
    joined = gpd.sjoin(points_df, polys_df, how='left', op='within')
    if 'index_right' in joined.columns:
        joined.drop(labels='index_right', axis=1, inplace=True)
    return joined


def snap_point_to_linear_ring(point, linear_ring):
    projection = linear_ring.project(point)
    new_point_coords = linear_ring.interpolate(projection)
    snapped_point = sly.geometry.Point(list(new_point_coords.coords)[0])
    snap_distance = point.distance(snapped_point)
    return (snap_distance, snapped_point)


def snap_points_to_polys_df(needs_snapping, polys_df, polys_location_col, descendents):
    geom_col = 'geometry'
    all_linear_rings = list()
    for poly_index, poly_row in polys_df.iterrows():
        poly = poly_row[geom_col]
        matching_col = poly_row[polys_location_col]
        if type(poly) is sly.geometry.polygon.Polygon:
            new_lin_rings = [[sly.geometry.LinearRing(poly.exterior.coords),
                              matching_col]]
        else:
            new_lin_rings = [[sly.geometry.LinearRing(i.exterior.coords),
                              matching_col]
                             for i in poly.geoms]
        all_linear_rings = all_linear_rings + new_lin_rings
    needs_snapping.loc[:, 'snapped_lat'] = np.nan
    needs_snapping.loc[:, 'snapped_lon'] = np.nan
    needs_snapping.loc[:, 'snap_dist'] = np.nan
    needs_snapping.loc[:, 'overlay_loc_id'] = np.nan
    for snap_index, snap_row in needs_snapping.iterrows():
        unsnapped_point = snap_row[geom_col]
        min_snap_dist = None
        snapped_point = None
        snap_name = None
        for lin_ring in all_linear_rings:
            (this_snap_dist, this_snap_pt) = snap_point_to_linear_ring(
                point=unsnapped_point,
                linear_ring=lin_ring[0])
            if (min_snap_dist is None) or (this_snap_dist < min_snap_dist):
                min_snap_dist = this_snap_dist
                snapped_point = this_snap_pt
                snap_name = lin_ring[1]
        snapped_coords = list(snapped_point.coords)[0]
        SNAP_UPPER_LIMIT = 1.5
        if min_snap_dist <= SNAP_UPPER_LIMIT:
            needs_snapping.loc[snap_index, 'snapped_lat'] = snapped_coords[1]
            needs_snapping.loc[snap_index, 'snapped_lon'] = snapped_coords[0]
            needs_snapping.loc[snap_index, 'snap_dist'] = min_snap_dist
            needs_snapping.loc[snap_index, 'overlay_loc_id'] = snap_name
    snapped = needs_snapping.copy()
    return snapped


def overlay_or_snap_points(point_df, poly_df, location_set_id=21,
                           update_snapped_points=True):
    assert np.all([type(i) is gpd.geodataframe.GeoDataFrame
                   for i in [point_df, poly_df]]), ("The point_df and poly_df "
                                                    "should both be geopandas GeoDataFrames")
    poly_df = poly_df.copy()
    point_df = point_df.copy()
    poly_df = poly_df.rename(columns={'location_id': 'overlay_loc_id'})
    meta = get_location_metadata(location_set_id=location_set_id, gbd_round_id=6)
    descendents = construct_descendants_dict(meta)
    point_df['known_loc_tag'] = 1
    reference_locations = [int(i) for i in list(descendents.keys())]
    if 'location_id_matched' in point_df.columns:
        point_df.loc[~np.isnan(point_df['location_id_matched']),
                     'known_loc_tag'] = point_df.loc[
                         ~np.isnan(point_df['location_id_matched']),
                         'location_id_matched'].apply(
                         lambda x: 1 if int(x) not in reference_locations else int(x))
    print("* * * * STARTING FIRST OVERLAY * * * * at {}".format(dt.now()))

    point_df['uid'] = point_df.index

    all_overlaid = overlay_polygons(points_df=point_df, polys_df=poly_df,
                                    polys_cols_to_join=['overlay_loc_id'])
    border_uids_df = all_overlaid.loc[:, ['uid']]
    border_uids_df['count'] = 1
    border_uids_df = (border_uids_df.groupby(by='uid')
                                    .sum()
                                    .reset_index(drop=False))
    border_uids = (border_uids_df.loc[border_uids_df['count'] == 2, 'uid']
                                 .tolist())
    print("  WARNING: The following uid are being duplicated at this stage:")
    print("  {}".format(border_uids))
    print("  These should be assigned beforehand to avoid duplication.\n")

    print("* * * * DONE WITH FIRST OVERLAY * * * * at {}".format(dt.now()))
    print(all_overlaid.shape)
    all_overlaid['good_match'] = all_overlaid.apply(
        lambda row:
        (row['overlay_loc_id'] is not np.nan) and
        (row['overlay_loc_id'] in descendents[row['known_loc_tag']]), axis=1)
    number_of_bad_matches = all_overlaid[all_overlaid['good_match'] == 0].shape[0]

    if number_of_bad_matches > 0:
        snap_points = True
    else:
        snap_points = False

    snap_points = False # 
    if not(snap_points):
        return all_overlaid
    overlaid_good = all_overlaid.loc[all_overlaid['good_match'], :].copy()
    needs_snapping = all_overlaid.loc[~all_overlaid['good_match'], :].copy()
    print("  {} points need to be snapped.".format(needs_snapping.shape[0]))

    print("* * * * * * * * CHECK DF SIZE * * * * * * * *")
    print("  {} points were good.".format(overlaid_good.shape[0]))
    print("  {} combined.".format(all_overlaid.shape[0]))
    print("* * * * STARTING SNAPPING * * * * at {}".format(dt.now()))
    snapped_sub_dfs = list()
    for parent_loc in needs_snapping['known_loc_tag'].dropna().unique().tolist():
        possible_snap_polys = poly_df.loc[(poly_df['overlay_loc_id'].isin(
            descendents[int(parent_loc)])), :]
        if len(possible_snap_polys) == 0:
            print("All location tagging failed for parent location: {}".format(parent_loc))
        points_to_snap = needs_snapping.loc[needs_snapping['known_loc_tag'] == parent_loc, :]
        snapped_sub = snap_points_to_polys_df(needs_snapping=points_to_snap,
                                              polys_df=possible_snap_polys,
                                              polys_location_col='overlay_loc_id',
                                              descendents=descendents)
        snapped_sub_dfs.append(snapped_sub)
    snapped = pd.concat(snapped_sub_dfs)
    snap_geom = [sly.geometry.Point(xy) for xy
                 in zip(snapped['snapped_lon'], snapped['snapped_lat'])]
    snapped = snapped.drop(labels=['geometry', 'snapped_lon', 'snapped_lat'],
                           axis=1)
    snapped = gpd.GeoDataFrame(snapped, crs={'init': 'epsg:4326'},
                               geometry=snap_geom)
    print("\n* * * * DONE WITH SNAPPING * * * * at {}".format(dt.now()))
    print("\n* * * * * * * * CONFIRM SNAP WORKED * * * * * * * *")
    print("  The snapped df now has {} rows (should be same)".format(snapped.shape[0]))
    print("  There are {} rows that still don't have a loc_id.".format(
          snapped.loc[snapped['overlay_loc_id'].apply(lambda x: x == ''), :].shape[0]))
    overlaid_good['snap_dist'] = 0
    all_geolocated = pd.concat([overlaid_good, snapped])
    meta_names = meta.loc[:, ['location_id', 'location_ascii_name']]
    meta_names.rename(columns={'location_id': 'overlay_loc_id',
                      'location_ascii_name': 'overlay_loc_name'}, inplace=True)
    all_geolocated = all_geolocated.merge(meta_names, on="overlay_loc_id", how='left')
    all_geolocated.drop(labels=['known_loc_tag'], axis=1, inplace=True)
    print("* * * * * * * * CHECK DF SIZE PRESERVED * * * * * * * *")
    print("  {} rows at the end (should be same as beginning).".format(all_geolocated.shape[0]))
    return(all_geolocated)

def return_national_location(row, locs):
    location_id = row['location_id']
    row = row.fillna("")
    detail_1 = (row['admin1'] == "")
    detail_2 = (row['admin2'] == "")
    detail_3 = (row['admin3'] == "")
    no_detail = False not in [detail_1, detail_2, detail_3]
    if row.at['location_id'] != '':
        if no_detail is True:
            loc_history = locs[locs["location_id"] == row.at['location_id']]
            loc_history = loc_history['path_to_top_parent'].iloc[0]
            loc_history = loc_history.split(",")
            national_id = loc_history[3]
            location_id = national_id
    return location_id


def generate_gps_matched_file(df, source, filename="{}_gps_location_map.csv",
                              version='test', artificial_coords=False):

    # set the source folder and make sure it exists
    input_folder = LOCATION_INPUT_FOLDER.format(source=source)
    input_archive_folder = os.path.join(input_folder, "archive")
    assert os.path.exists(input_folder), ("you are missing the source input folder")
    cols = ['source_event_id', 'country', 'admin1', 'admin2', 'admin3', 'latitude', 'longitude']

    if "query" in df.columns:
        cols += ["query"]
    in_df = df.copy()
    print(in_df.shape)
    in_df = in_df[cols]
    in_df = in_df.dropna(how='any', subset=['latitude', 'longitude'])
    locs = get_location_metadata(location_set_id=21)
    if in_df.shape[0] > 0:
        points = df_to_points_gdf(in_df, drop_lat_lon=False)
        shp = load_master_shapefile()
        print(in_df.shape)
        all_geolocated = overlay_or_snap_points(point_df=points, poly_df=shp)
        all_geolocated = all_geolocated.drop(labels=['geometry'], axis=1)
        all_geolocated = all_geolocated.drop(
            labels=['location_id_matched'], axis=1, errors='ignore')
        all_geolocated = all_geolocated.rename(columns={'overlay_loc_id': 'location_id'})
        print(all_geolocated.shape)
        map_path = os.path.join(input_folder, filename.format(source))
        map_path_archive = os.path.join(input_archive_folder,
                                        (filename.format(source).strip(".csv") +
                                         "_{}.csv".format(version)))
        if artificial_coords is True:
            all_geolocated['location_id'] = all_geolocated.apply(return_national_location,
                                                                 locs=locs,
                                                                 axis=1)
        print(all_geolocated.shape)
        all_geolocated['location_id'] = all_geolocated['location_id'].apply(lambda x: str(x))
        all_geolocated = all_geolocated.groupby(['source_event_id','country','admin1','admin2','admin3'], as_index=False)['location_id'].agg({'location_id':', '.join})
        all_geolocated['side_a'] = ""
        all_geolocated['side_b'] = ""
        print(all_geolocated.shape)
        all_geolocated = all_geolocated.dropna(subset=['location_id'])
        print(all_geolocated.shape)
        all_geolocated.to_csv(map_path, index=False)
        all_geolocated.to_csv(map_path_archive, index=False)

    else:
        print("No events have latitude and longitude to overalay")

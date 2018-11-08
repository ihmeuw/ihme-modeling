# Imports (general)
import argparse
import numpy as np
import pandas as pd
from datetime import datetime as dt
from os.path import join
from sys import platform
import warnings

import shapely as sly
import geopandas as gpd

from db_queries import get_location_metadata

def df_to_points_gdf(in_data, lat_col='latitude', lon_col='longitude',
                     drop_lat_lon=False):
    '''
    Converts a DataFrame with two fields representing latitude and longitude
    into a GeoDataFrame

    Inputs:
      in_data (pandas DataFrame): DataFrame of points to geocode
      lat_col (str): Field in in_data containing VALID latitudes
      lon_col (str): Field in in_data containing VALID longitudes
      drop_lat_lon (bool): Whether or not to drop the latitude and longitude
        data from the original input dataframe

    Output:
      points_df (geopandas GeoDataFrame): GeoDataFrame containing a new field,
        'geometry', that contains Shapely Points objects
    
    '''    
    # Input data validation
    assert type(in_data) is pd.core.frame.DataFrame, "'in_data' was not a DataFrame"
    assert np.all([i in in_data.columns for i in [lat_col, lon_col]]), "Missing lat/long columns"
    assert "geometry" not in in_data.columns, ("'geometry' cannot already be a"
                       " column in the dataframe - this will be overwritten")
    # Turn each latitude and longitude pair into a shapely Point object
    geometry = [sly.geometry.Point(xy) for xy in zip(in_data[lon_col],in_data[lat_col])]
    # If specified, drop the input latitude and longitude fields
    if drop_lat_lon:
        in_data.drop(labels=[lon_col,lat_col], axis=1, inplace=True)
    # Create a GeoDataFrame containing points for each observation
    points_df = gpd.GeoDataFrame(in_data,
                                  crs={'PASSWORD'},
                                  geometry=geometry)
    return points_df


def load_master_shapefile(location_set_id=21, gbd_round_id=5,
            shp_filepath=("FILEPATH")):
    '''
    Input:
      location_set_id (int): GBD location set id for the metadata (default is 
        21 = Mortality Computation location set)
      gbd_round_id (int): GBD round id for the metadata (default is 4 = GBD 2016)
      shp_filepath (str): The filepath to the shapefile

    Output:
      shp (geopandas GeoDataFrame): The GBD analysis shapefile, merged on
        the specified metadata
    '''
    # Read in the shapefile
    shp = gpd.read_file(shp_filepath)
    # Rename the 'loc_id' field to 'location_id'
    shp.rename(columns={"loc_id":"location_id"},inplace=True)
    # Get location hierarchy information for the given location set version
    meta = get_location_metadata(location_set_id=location_set_id,
                                 gbd_round_id=gbd_round_id)

    merge_cols = ['location_id','path_to_top_parent','most_detailed']
    shp_joined = shp.merge(meta.loc[:,merge_cols],
                           on=["location_id"], how='inner')
    dropped_rows = shp.shape[0] - shp_joined.shape[0]
    if dropped_rows > 0:
        missing_rows = [i for i in shp['location_id'].unique().tolist()
                          if i not in shp_joined['location_id'].unique().tolist()]
        print("{} locations were dropped from the master "
              "shapefile due to the merge: {}".format(dropped_rows,missing_rows))
    shp_joined = shp_joined.loc[shp_joined['most_detailed']==1,:]
    dropped_rows = shp.shape[0] - shp_joined.shape[0]
    if dropped_rows > 0:
        print("{} locations were dropped because they were not most detailed"
                                                        .format(dropped_rows))
    # Return the merged shapefile
    shp_joined = shp_joined.drop(labels='most_detailed',axis=1)
    return shp_joined


def overlay_polygons(points_df,
                     polys_df,
                     polys_cols_to_join):
    '''
    Overlays polygons onto a given points dataframe
    
    Inputs:
      points_df (geopandas GeoDataFrame): a Points geodataframe
      polys_df (geopandas GeoDataFrame): a Polygons geodataframe to spatially 
        join with the points GeoDataFrame. This shapefile will be read in and 
        then merged, keeping only the desired join columns
      polys_cols_to_join (list of str) = List of fields in the shapefile data
        to add back to the original point data

    Output:
      joined (geopandas GeoDataFrame): A spatially-enabled 
    '''
    # In data prep
    if type(polys_cols_to_join) is not list:
        polys_cols_to_join = [polys_cols_to_join]
    assert np.all([i in polys_df.columns for i in polys_cols_to_join]),("Not all"
                    " items in {} are column names in the polys_df".format(polys_cols_to_join))
    polys_df = polys_df.loc[:,['geometry'] + polys_cols_to_join]
    # OVERLAY OPERATION
    # This command will combine the points with spatial data from the overlapping
    # polygon shapefile
    joined = gpd.sjoin(points_df,polys_df,how='left',op='within')
    if 'index_right' in joined.columns:
        joined.drop(labels='index_right', axis=1, inplace=True)
    return joined


def construct_descendants_dict(loc_meta):
    '''
    Helper function for overlay_or_snap_points. Given full location metadata
    for a location set, determines the MOST DETAILED descendents of all 
    locations that are NOT most detailed.
    '''
    # Get a list of all locations that are not most detailed
    not_most_detailed = loc_meta.loc[loc_meta['most_detailed']==0,
                                     'location_id'].tolist()
    detailed_df = loc_meta.loc[loc_meta['most_detailed']==1,:]
    descendents = dict()
    #removing global
    not_most_detailed = [i for i in not_most_detailed if i!=1]
    descendents[1] = detailed_df['location_id'].tolist()
    # Iterate through all other cases
    for parent_id in not_most_detailed:
        parent_text = ",{},".format(parent_id)
        descendents[parent_id] = detailed_df.loc[
            detailed_df['path_to_top_parent'].apply(lambda x: parent_text in x),
                        'location_id'].tolist()
    # Let every most detailed item be a descendent of itself
    for detailed_id in detailed_df['location_id'].tolist():
        descendents[detailed_id] = [detailed_id]
    # Return the full dictionary
    return descendents


def snap_point_to_linear_ring(point, linear_ring):
    '''
    Snaps a SINGLE point to the closest point on a SINGLE polygon boundary, 
    returning the closest point on the polygon and the distance from the 
    original point

    Inputs:
      point (shapely Point class): The point to be snapped
      linear_ring (shapely LinearRing): The linear ring to snap to

    Returns:
      snap_distance (float): The distance from the original point to the
        snapped point
      snapped_point (shapely Point class): The snapped point
    '''
    # Project the point onto the linear ring to get the closest location
    #  on its surface
    projection = linear_ring.project(point)
    new_point_coords = linear_ring.interpolate(projection)
    # Create a new point bases on the closest point on the linear ring's surface
    snapped_point = sly.geometry.Point(list(new_point_coords.coords)[0])
    snap_distance = point.distance(snapped_point)
    return (snap_distance, snapped_point)


def snap_points_to_polys_df(needs_snapping, polys_df, polys_location_col, descendents):
    '''
    Snaps all points in a points GeoDataFrame to one of a given set of polygons.

    Inputs:
      needs_snapping (gpd GeoDataFrame): points geodataframe to be snapped
      polys_df (gpd GeoDataFrame): Polygon geodataframe including all polygons
        that can be snapped to
      polys_location_col: The field from the polys_df that should be assigned 
        to each row in needs_snapping
      descendents (dict): A dictionary matching a location to all of its most
        detailed children

    Outputs:
      snapped (gpd GeoDataFrame): points geodataframe containing referenced values
        from the polys_location_col in the polys_df, as well as updated points
        that fall within the matched polygon
    '''
    # First, iterate through all rows of the polys geodataframe to create a list
    #  of all geometries (as LinearRing objects)
    geom_col = 'geometry'
    all_linear_rings = list()
    for poly_index, poly_row in polys_df.iterrows():
        poly = poly_row[geom_col]
        matching_col = poly_row[polys_location_col]
        if type(poly) is sly.geometry.polygon.Polygon:
            # The shape is a polygon; make it the only feature in a list
            new_lin_rings = [[sly.geometry.LinearRing(poly.exterior.coords),
                              matching_col]]
        else:
            # Get every polygon in the MultiPolygon
            new_lin_rings = [[sly.geometry.LinearRing(i.exterior.coords),
                              matching_col]
                             for i in poly.geoms]
        all_linear_rings = all_linear_rings + new_lin_rings
    # We now have a list of linear rings that we can iterate through and snap to points
    # Create new columns in the points dataframe
    needs_snapping.loc[:,'snapped_lat']    = np.nan
    needs_snapping.loc[:,'snapped_lon']    = np.nan
    needs_snapping.loc[:,'snap_dist']      = np.nan
    needs_snapping.loc[:,'overlay_loc_id'] = np.nan
    # Iterate through the points
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
        # After going through all snapped polygons, assign the best point as
        #  the matched point
        # This point should have a fixed upper limit of 1.5 decimal degrees
        snapped_coords = list(snapped_point.coords)[0]
        SNAP_UPPER_LIMIT = 1.5
        if min_snap_dist <= SNAP_UPPER_LIMIT:
            needs_snapping.loc[snap_index,'snapped_lat'] = snapped_coords[1]
            needs_snapping.loc[snap_index,'snapped_lon'] = snapped_coords[0]
            needs_snapping.loc[snap_index,'snap_dist'] = min_snap_dist
            needs_snapping.loc[snap_index,'overlay_loc_id'] = snap_name
        print(".",end="")
    snapped = needs_snapping.copy()
    return snapped


def overlay_or_snap_points(point_df, poly_df, location_set_id=21,
                           snap_points=True,
                           update_snapped_points=True):
    '''
    This function takes a geopandas Points GeoDataFrame and Polygons
    GeoDataFrame, then assigns all rows in the Points GeoDataFrame to a single
    polygon in the Polygons GeoDataFrame. It checks for exact overlap, then
    snaps points that fall outside of any polygon (or points that already have 
    identifying information that indicates they do not belong in their current
    polygon).

    Inputs:
      point_df (geopandas GeoDataFrame): The points GeoDataFrame
      poly_df (geopandas GeoDataFrame): The polygons GeoDataFrame
      location_set_id (int): The value of the GBD location set that will be 
        used to determine which most detailed locations align with which (not
        necessarily most detailed) parents
      snap_points (bool): Whether or not to snap points in addition to the overlay
      update_snapped_points (bool): If true, drop the old set of points and 
        update the 'geometry' field of the points gdf to the new, snapped points

    Outputs:
      all_geolocated (geopandas GeoDataFrame): The points GeoDataFrame, where
        a new field "overlay_loc_id" indicates the polygon that the point
        overlaps with or has been snapped to
    '''
    # Input data validation
    assert np.all([type(i) is gpd.geodataframe.GeoDataFrame 
                   for i in [point_df, poly_df]]),("The point_df and poly_df"
                                   " should both be geopandas GeoDataFrames")
    # Copy the original data to allow for in-place changes
    poly_df = poly_df.copy()
    point_df = point_df.copy()
    # Rename the polygon field 'location_id' so it does not overlap with the
    #  points field 'location_id'
    poly_df = poly_df.rename(columns={'location_id':'overlay_loc_id'})
    # Get location metadata for the known location set
    meta = get_location_metadata(location_set_id=location_set_id)
    # Create a dictionary of the most detailed descendents for each location
    descendents = construct_descendants_dict(meta)
    # Create a field that will be used to validate whether a point has been placed
    #  within a valid geometry
    point_df['known_loc_tag'] = 1
    reference_locations = [int(i) for i in list(descendents.keys())]
    if 'location_id_matched' in point_df.columns:
        point_df.loc[~np.isnan(point_df['location_id_matched']),
                     'known_loc_tag'] = point_df.loc[
                         ~np.isnan(point_df['location_id_matched']),
                         'location_id_matched'].apply(lambda x: 
                             1 if int(x) not in reference_locations
                               else int(x))
    ## Overlay points

    print("* * * * STARTING FIRST OVERLAY * * * * at {}".format(dt.now()))

    all_overlaid = overlay_polygons(points_df=point_df,polys_df=poly_df,
                                    polys_cols_to_join=['overlay_loc_id'])
    # Check if there were any UIDs on the border that might be duplicated
    border_uids_df = all_overlaid.loc[:,['uid']]
    border_uids_df['count'] = 1
    border_uids_df = (border_uids_df.groupby(by='uid')
                                    .sum()
                                    .reset_index(drop=False))
    border_uids = (border_uids_df.loc[border_uids_df['count']==2,'uid']
                                 .tolist())
    print("  WARNING: The following UIDs are being duplicated at this stage:")
    print("  {}".format(border_uids))
    print("  These should be assigned beforehand to avoid duplication.\n")

    print("* * * * DONE WITH FIRST OVERLAY * * * * at {}".format(dt.now()))
    
    # Subset out points that have not been matched to a geography or were
    #  matched to an impossible geometry per the 'valid geometry' field
    all_overlaid['good_match'] = all_overlaid.apply(lambda row:
             (row['overlay_loc_id'] is not np.nan) and
             (row['overlay_loc_id'] in descendents[row['known_loc_tag']]),
             axis=1)
    # If we don't want to snap points, then return the points here
    if not(snap_points):
        return all_overlaid
    # Otherwise, continue on to snapping
    overlaid_good = all_overlaid.loc[all_overlaid['good_match'],:].copy()
    needs_snapping = all_overlaid.loc[~all_overlaid['good_match'],:].copy()
    print("  {} points need to be snapped.".format(needs_snapping.shape[0]))

    print("* * * * * * * * CHECK DF SIZE * * * * * * * *")
    print("  {} points were good.".format(overlaid_good.shape[0]))
    print("  {} combined.".format(all_overlaid.shape[0]))


    # Iterate through each parent geometry, getting the best fit out of all 
    #  descendants of that parent geometry. Afterwards, concatenate the results
    #  from all parents into a single dataframe
    print("* * * * STARTING SNAPPING * * * * at {}".format(dt.now()))

    snapped_sub_dfs = list()
    for parent_loc in needs_snapping['known_loc_tag'].dropna().unique().tolist():
        possible_snap_polys = poly_df.loc[(poly_df['overlay_loc_id']
                                            .isin(descendents[int(parent_loc)])),:]
        if len(possible_snap_polys) == 0:
            warnings.warn("All location tagging failed for parent location: {}".format(parent_loc))
            continue
        points_to_snap = needs_snapping.loc[needs_snapping['known_loc_tag']==parent_loc,:]
        snapped_sub = snap_points_to_polys_df(needs_snapping=points_to_snap,
                                              polys_df=possible_snap_polys,
                                              polys_location_col='overlay_loc_id',
                                              descendents=descendents)
        snapped_sub_dfs.append(snapped_sub)
    snapped = pd.concat(snapped_sub_dfs)
    # Update with the new, snapped points as the geometry
    snap_geom = [sly.geometry.Point(xy) for xy
                 in zip(snapped['snapped_lon'],snapped['snapped_lat'])]
    snapped = snapped.drop(labels=['geometry','snapped_lon','snapped_lat'],
                           axis=1)
    snapped = gpd.GeoDataFrame(snapped, crs={'PASSWORD'},
                                  geometry=snap_geom)
    print("\n* * * * DONE WITH SNAPPING * * * * at {}".format(dt.now()))
    print("\n* * * * * * * * CONFIRM SNAP WORKED * * * * * * * *")
    print("  The snapped df now has {} rows (should be same)".format(snapped.shape[0]))
    print("  There are {} rows that still don't have a loc_id.".format(
          snapped.loc[snapped['overlay_loc_id'].apply(lambda x: x == ''),:].shape[0]))

    # Snapping will add the column "snap_dist"
    #  to the geodataframe. Make these consistent with the overlaid df and
    #  concatenate
    overlaid_good['snap_dist'] = 0
    all_geolocated = pd.concat([overlaid_good, snapped])

    # Add a field giving the location name that each point is now assigned to
    meta_names = meta.loc[:,['location_id','location_ascii_name']]
    meta_names.rename(columns={'location_id':'overlay_loc_id',
                      'location_ascii_name':'overlay_loc_name'}, inplace=True)
    all_geolocated = all_geolocated.merge(meta_names, on="overlay_loc_id", how='left')
    # Delete the field that was used to determine valid locations for snapping
    all_geolocated.drop(labels=['known_loc_tag'], axis=1, inplace=True)

    print("* * * * * * * * CHECK DF SIZE PRESERVED * * * * * * * *")
    print("  {} rows at the end (should be same as beginning).".format(all_geolocated.shape[0]))
    # Return the dataframe
    return(all_geolocated)


##############################################################################
# MAIN FUNCTION
##############################################################################
def create_gdf_overlay_with_shp(in_df, lat_col, lon_col, drop_lat_lon,
                                snap_points=True):
    '''
    This is a wrapper function that encompasses the entire overlay process.
    Given an input dataframe with latitude and longitude columns, the function
    creates a geopandas Points Geodataframe, loads the GBD master shapefile,
    and then overlays the Points data with that shapefile, snapping
    non-overlapping points if desired.
    
    Input:
      in_df: The input dataframe
      lat_col: Valid latitude column within the in_df
      lon_col: Valid longitude column with the in_df
      drop_lat_lon: Determines whether or not to drop the latitude and
        longitude columns in the in_df once they have been used to create the
        points geodataframe
      snap_points: Determines whether or not to snap points that do not fall
        within a known correct polygon

    Output:
      all_geolocated: Pandas dataframe containing a new field,
        'location_id_matched', that designates the location ID of the overlaid
        shapefile
    '''
    # Convert input dataframe to Points GeoDataFrame
    points = df_to_points_gdf(in_data=in_df,drop_lat_lon=drop_lat_lon,
                              lat_col=lat_col,lon_col=lon_col)
    # Read in the GBD analysis shapefile with metadata attached
    shp = load_master_shapefile()
    # Run polygon snapping
    all_geolocated = overlay_or_snap_points(point_df=points, poly_df=shp,
                                            snap_points=snap_points)
    # Convert the geodataframe back to a pandas DataFrame
    all_geolocated = all_geolocated.drop(labels=['geometry'],axis=1)
    # Rename the column to 'location_id_matched' to be standard with other
    #  outputs of location matching
    all_geolocated = all_geolocated.drop(labels=['location_id_matched'], axis=1,
                                         errors='ignore')
    all_geolocated = all_geolocated.rename(
                               columns={'overlay_loc_id':'location_id_matched'})
    return all_geolocated


if __name__=="__main__":
    # Set up argument parsing for input and output files
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--infile",type=str,
                        help="The filepath of the points data csv")
    parser.add_argument("-o","--outfile",type=str,
                        help="The filepath of the snapped points")
    parser.add_argument("-x","--longitude_col",type=str,default='longitude',
                        help="The column in the points data csv identifying longitude")
    parser.add_argument("-y","--latitude_col",type=str,default='latitude',
                        help="The column in the points data csv identifying latitude")
    parser.add_argument("-n","--nosnap",action="store_true",
                        help="Add this argument to overlay points"
                             " but NOT perform snapping.")
    # Parse arguments
    cmd_args = parser.parse_args()
    assert cmd_args.infile is not None, "Overlay program requires an input filepath"
    assert cmd_args.outfile is not None, "Overlay program requires an output filepath"
    # Read in the points data and convert to a geopandas Points dataframe
    in_data = pd.read_csv(cmd_args.infile,encoding='latin1')
    # Run main processing
    overlaid = create_gdf_overlay_with_shp(in_df=in_data,
                                lat_col=cmd_args.latitude_col,
                                lon_col=cmd_args.longitude_col,
                                drop_lat_lon=False,
                                snap_points=not(cmd_args.nosnap))
    # Save as a pandas dataframe
    overlaid.to_csv(cmd_args.outfile,encoding='latin1',index=False)
    
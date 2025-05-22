"""
The functions in this script are used in the age sex splitting step. After the
age sex splitting is performed the data is aggregated to the national level.

Typtically the functions in this file are imported and this file isn't run as a
script.

It only provides aggregations if all the necessary subnationals are present.
"""

import sys
import getpass
import pandas as pd
import numpy as np
import gbd

sys.path.append("FILEPATH")
from cod_prep.downloaders import get_current_location_hierarchy

new_run_id = sys.argv[1]


def aggregate_to_country_level(orig_df, nid_map, gbd_round_id, release_id):
    """
    Main function that aggregates subnationals up to national level.

    Args:
        orig_df (Pandas DataFrame): DataFrame that contains subnational data
            to be aggregated
        nid_map (str): Full filepath to file that contains a mapping from
            subnational NID to national aggregate merged NID. This is for the
            cases where subnational locations from separate sources need to be
            aggregated together.
            Aggregation to national requires an nid change in cases where
            data from  separate sources need to be used to make a national
            aggregate. For example, in for the aggregate that makes the UK
            data, we have to aggregate data from Wales, Scotland,
            Northern Ireland, and England, that all came from different sources
            and have different NIDs. If the nids from the subnational locations
            were left unchanged, their NIDs would distinguish them and they
            would not aggregate together in the collapse.

    """
    output_cols = ["age_group_id", "deaths", "location_id", "nid",
                   "underlying_nid", "sex_id", "source", "year_id",
                   "estimate_stage_id", "source_type_id", "is_agg"]

    # Copy just the subnationals to a separate dataframe
    locs_df = get_current_location_hierarchy(location_set_id=82, location_set_version_id=1150, release_id=release_id)  
    locs_df = locs_df[["location_id", "parent_id", "level", "ihme_loc_id",
                       "path_to_top_parent"]]
    locs_df["country"] = locs_df.apply((lambda row: row["ihme_loc_id"][:3]), axis=1)

    master_df = pd.merge(orig_df.copy(), locs_df, on="location_id")
    master_df["is_agg"] = 0
 
    # we don't want to aggregate China mainland, Hong Kong and
    # Macao to China level
    subs_df = master_df.loc[(master_df["level"] > 3) &
                            (~master_df["location_id"].isin([44533, 354, 361]))].copy()

    countries_df = master_df.loc[(master_df["level"] <= 3) |
                                 (master_df["location_id"].isin([44533, 354, 361]))].copy()
    countries_df = countries_df[output_cols]

    parent_countries = subs_df["country"].unique().tolist()
    print("Aggregating subnationals for these countries: " + ", ".join(parent_countries))

    # For each country group:
    def agg_children(parent):
        """
        Function that selects child locations and aggregates them up one level.
        """
        # Subset to just subnationals belonging to the parent country
        children = subs_df.loc[subs_df["country"] == parent]
        print("Aggregating " + parent + "...")

        # Subset to just the necessary columns
        children = children[output_cols + ["parent_id", "level"]]

        # Start at the lowest level
        agg_level = max(children["level"])

        # don't want to aggregate up to china
        if parent == "CHN":
            national_level = 4
        else:
            national_level = 3

        # Loop through each level of subnationals
        while agg_level > national_level:
            children_to_agg = children.loc[children["level"] == agg_level].copy()
            loc_counts = len(locs_df.loc[(locs_df.country == parent) & (locs_df.level == agg_level)])
            # count subnational locations for each location - year - source_type_id
            children_to_agg["subnat_count"] = children_to_agg.groupby(by=["year_id", "source_type_id", "estimate_stage_id"])['location_id'].transform("nunique")
            # keep data with all subnats present
            children_to_agg = children_to_agg[children_to_agg.subnat_count == loc_counts]

            if children_to_agg.shape[0] == 0:
                print("Skip aggregation.")
                break

            # Throw an error if duplicate values are encountered
            # source type id should be included because we can have more than one source for a demographic
            index = ["age_group_id", "sex_id", "year_id", "location_id",
                     "source_type_id", "estimate_stage_id"]
            children_to_agg["dup"] = children_to_agg.duplicated(subset=index, keep = False)
            #drop aggregated data if there's raw data at that level
            children_to_agg = children_to_agg[~((children_to_agg.dup == True) &
                                              (children_to_agg.is_agg == 0))]
            dups = children_to_agg.duplicated(subset=index)
            error_msg = "{d} duplicates encountered in {parent} data at aggregation level {level}".format(d=dups.sum(), parent=parent, level=agg_level)
            assert dups.sum() == 0, error_msg

            ## Merge on the country level nids and check if any of the subnational nids need to be replaced
            pre = children_to_agg.shape[0]
            # Aggregation to national requires an nid change in cases where data
            # from separate sources need to be used to make a national
            # aggregate, e.g., in the UK. 
            children_to_agg = pd.merge(children_to_agg, nid_map, left_on=["nid", "year_id"], right_on=["subnational_nid", "year_id"], how="left")
            assert children_to_agg.shape[0] == pre, "number of rows changed while merging on new nids."
            # exception: don't want to change the nid if it's aggregating england UTLAs up to regions or up to england
            england_locs = locs_df[locs_df.path_to_top_parent.str.contains(",4749,")].location_id.unique().tolist()
            england_locs.append(4749)  # have to add england itself to the list
            # this is a row wise operation that says set nid to country_nid if country_nid is not null AND that row isn't an england location, else, set nid to nid.
            children_to_agg["nid"] = children_to_agg.apply((lambda row: row["country_nid"] if pd.notnull(row["country_nid"]) and row["location_id"] not in england_locs else row["nid"]), axis=1)
            children_to_agg["source"] = children_to_agg.apply((lambda row: row["country_source"] if pd.notnull(row["country_source"]) and row["country_nid"] not in england_locs else row["source"]), axis=1)
            children_to_agg = children_to_agg.drop(["country_nid", "country_source", "subnational_nid"], axis=1)

            # Make sure nids/sources aren't being dropped
            def safe_agg(values):
                """Returns 1st value in list of unique values in a column"""
                values = values.unique().tolist()
                
                return(values[0])

            # Here's the actual aggregation up to the next location level.
            children_to_agg = children_to_agg.groupby(["age_group_id", "source_type_id", "estimate_stage_id","sex_id", "year_id", "parent_id"], as_index=False).agg({"deaths": sum, "nid": safe_agg, "source": safe_agg})
            
            # Replace the old location_id with the parent_id
            children_to_agg.rename(columns={"parent_id": "location_id"},
                                   inplace=True)

            # Mark as aggregates
            children_to_agg["is_agg"] = 1

            # Add the aggregations to the dataset
            children = pd.concat([children, children_to_agg], sort=False)
            
            # Get rid of old location metadata
            children = children[output_cols]

            # Re-merge on location information based on new location id
            children = pd.merge(children, locs_df, on="location_id")
            children = children[output_cols + ["level", "parent_id"]]

            # Increment agg_level for the next loop
            agg_level = agg_level - 1

        # Subset to just the columns we want in the output
        children = children[output_cols]
        return children

    # Aggregating subnationals of parent countries
    agg_df = pd.concat([agg_children(parent) for parent in parent_countries],sort=False)
    
    # Adding aggregates onto the original data
    agg_df = pd.merge(agg_df, locs_df, on="location_id")
    agg_df = agg_df[output_cols]
    master_df = pd.concat([agg_df, countries_df],sort=False)

    # Drop any duplicates introduced by aggregation
    master_df["count"] = 1
    duplicated = master_df.copy().groupby(["location_id", "year_id", "sex_id",
                                           "age_group_id", "source_type_id", 
                                           "estimate_stage_id"],
                                          as_index=False)[["count"]].sum()
    master_df = master_df.drop(["count"], axis=1)
    master_df = pd.merge(master_df,
                         duplicated,
                         on=["location_id", "year_id", "sex_id",
                             "age_group_id", "source_type_id", "estimate_stage_id"])

    unique_source = master_df.loc[master_df["count"] == 1].copy()
    duplicate_source = master_df.loc[master_df["count"] > 1].copy()

    # Default behavior is to keep national aggregates of subnationals
    # over raw nationals
    duplicate_source["keep"] = duplicate_source["is_agg"]

    # Manually choose sources to keep:
    # CHN_DSP 1991-1995 and 2012 we want to keep original national values
    duplicate_source.loc[(duplicate_source.year_id.isin([1991,1992,1993,1994,1995,2002,2012])) &
                         (duplicate_source.source == "CHN_DSP") & (duplicate_source.is_agg == 0) & 
                         (duplicate_source.location_id == 44533), "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id.isin([1991,1992,1993,1994,1995,2002,2012])) &
                         (duplicate_source.source == "CHN_DSP") & (duplicate_source.is_agg == 1) & 
                         (duplicate_source.location_id == 44533), "keep"] = 0

    # 2000 China mainland: keep 2002 CHN Statistical Yearbook
    duplicate_source.loc[(duplicate_source.year_id == 2000) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "2002 CHN Statistical Yearbook"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2000) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "CHN_PROV_CENSUS"),
                         "keep"] = 0

    # 2010 China mainland: keep 2010 census tables
    duplicate_source.loc[(duplicate_source.year_id == 2010) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "2010 census tables"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2010) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "CHN_PROV_CENSUS"),
                         "keep"] = 0

    # 2004-2010 China mainland: keep 3rdNatlDSP
    duplicate_source.loc[(duplicate_source.year_id >= 2004) &
                         (duplicate_source.year_id <= 2010) &
                         (duplicate_source.source_type_id == 22) &
                         (duplicate_source.source == "3rdNatlDSP"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id >= 2004) &
                         (duplicate_source.year_id <= 2010) &
                         (duplicate_source.source_type_id == 22) &
                         (duplicate_source.source == "CHN_DSP"),
                         "keep"] = 0

    # UK 2016: keep allcause VR
    duplicate_source.loc[(duplicate_source.year_id == 2016) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.source == "England_2016_allcause_VR"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2016) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.source == "ICD10_UK_2001_2011"),
                         "keep"] = 0

    #ZAF 2011: keep DYB
    duplicate_source.loc[(duplicate_source.year_id == 2011) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "DYB"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2011) &
                         (duplicate_source.source_type_id == 5) &
                         (duplicate_source.source == "12146#stats_south_africa 2011 census"),
                         "keep"] = 0
                         
    #ITA 2020: keep Eurostat
    duplicate_source.loc[(duplicate_source.year_id == 2020) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.source == "EUROSTAT_ITA"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2020) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.nid == 494866),
                         "keep"] = 0
    
    #ITA 2021: keep Eurostat
    duplicate_source.loc[(duplicate_source.year_id == 2021) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.source == "EUROSTAT_ITA"),
                         "keep"] = 1
    duplicate_source.loc[(duplicate_source.year_id == 2021) &
                         (duplicate_source.source_type_id == 1) &
                         (duplicate_source.nid == 465535),
                         "keep"] = 0
     
    duplicate_source.to_csv("FILEPATH", index = False)
    duplicate_source = duplicate_source[duplicate_source.keep == 1]
    
    final_df = pd.concat([unique_source, duplicate_source],sort=False)
    final_df.drop(["is_agg", "keep"], axis=1)
    duplicates = final_df.duplicated(subset=["location_id", "year_id",
                                             "sex_id", "age_group_id",
                                             "source_type_id", "estimate_stage_id"], keep=False)
 
    assert not duplicates.any(), "Duplicates found"
    
    final_df = final_df[output_cols]
    print("Done!")

    return(final_df)


if __name__ == '__main__':
    # this file typically is not run as a script but imported as a function
    input_data = pd.read_csv("FILEPATH")
    input_data.underlying_nid = input_data.underlying_nid.replace(np.nan, 99999) #replace null underlying nids with 99999
    nid_map = pd.read_csv("FILEPATH")

    output_data = aggregate_to_country_level(input_data, nid_map)
    output_data.underlying_nid = output_data.underlying_nid.replace(99999, np.nan) #swap it back

    print("Writing {nrows} rows to current working directory!".format(nrows=output_data.shape[0]))
    output_data.to_csv("FILEPATH", index=False)

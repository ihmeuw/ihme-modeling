import arcpy

# Creation of the 2016 Pf masking layer

# All input layers have been preprocessed to have matching coordinate system, resolution, and coastline template.

# Reclassify the travel-health-guidelines zero Pf mask (from 2010 work) to remove nodata (which otherwise affects the following overlay)
arcpy.gp.Reclassify_sa("ithgz_pf_CLEAN.tif", "VALUE", "-9999 1;0 0;NODATA -1", "FILEPATH/ithgz_pf_CLEAN_no_nodata.tif", "DATA")

# Limits is where temp suitablity = 1 AND arid = 0 AND NOT excluded by ITHG layer
arcpy.gp.RasterCalculator_sa("""Con("ithgz_pf_CLEAN_no_nodata.tif" == 0,0,1) * "TempSuitability.Pf.limits.1k.2010.global.tif" * Con("Aridity_Mask_From_Globcover.tif" == 0,1,0)""", "FILEPATH/pf_limits.tif")
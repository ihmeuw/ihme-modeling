1. download_client_arg.py downloads each requested month of data with a separate job (after checking that it hasn't already been download) then calls
2. clean_client.py, which cleans the data, produces the daily mean temperature, dewpoint, and heat index for each pixel, then saves the data in a pandas-friendly csv.
3. world_attach_all, attach_world_subnats, etc, attach population and various other metadata to the pixels


### 
4. extraxt_ERA5 (and parent script) extract daily pixel values for each location (code is parallelized by year and location)
5. annual_mean01.r created multi year pixel means 
6. annual_mean01.r created location means (subnationals) from multi annual pixel means 


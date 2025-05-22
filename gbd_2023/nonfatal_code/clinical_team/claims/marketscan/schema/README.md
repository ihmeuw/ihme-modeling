### Description
Code to transform the raw MS sas files into first parquet files for easier access with Spark and then into an ICD-mart schema which allows for quick and easy access to 3 digit ICD groups.


### How to run the code:
In an interactive session, submit the jobmon workflow with the following. <br>
`from marketscan.schema.convert_new_ms_years import convert_ms` <br>
`wr = convert_ms([<years>])`

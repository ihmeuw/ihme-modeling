# ITA_HID Data Formatting
Latest update: Nov 2023

For previous versions of ITA formatting, please see the README in the `_archive` folder.


We've confirmed that we can reliably drop day cases based on the `reg_ric` column/variable in the raw data we were given.

Now with ezjobmon implemented, to format this source as is, we can simply run the `format_ITA_HID.py` script.
> python format_ITA_HID.py

This script formats and saves the output by year and also combines all outputs to `FILEPATH` if the workflow finishes successfully.

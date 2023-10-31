# `winnower`

## Upgrading

If `winnower` is already installed you can easily upgrade.

1. Open your **Anaconda Prompt**
1. Type `activate winnower` and hit enter
1. Type `pip install --upgrade --extra-index-url=<ADDRESS> --trusted-host <ADDRESS> winnower` and hit enter

## Installation

If you already have Anaconda (or Conda) installed you just need to open an _Anaconda Prompt_ and run this command:

```
pip install --extra-index-url=<ADDRESS> --trusted-host <ADDRESS> winnower
```

If you don't have Anaconda installed please see [the install guide](INSTALL.md)

## Using `winnower`

*NOTE*: most topics with custom code are not yet supported. Please check with `<USERNAME>` to confirm your topic is working.

```bash
# Be sure to activate your environment before trying to run_extract
# E.g., on Windows open an Anaconda Prompt and "activate winnower"
#       on Linux/OS X open a terminal and `source activate winnower`

# Extract ubcov_id 15 and 27 with design, demographics, geography, wash, and alcohol topics
run_extract --topic alcohol --topic wash 15 27

# Save file in J:\temp\ instead of the current directory
run_extract -C J:\temp\  -o my-extraction.csv 15

# Extract ubcov_id 15 but save it in a directory named after the country, as NID_extraction.csv
# See run_extract --help for the full list of supported fields
run_extract 15 -o "{iso3}/{nid}_extraction.csv"

# Get help details
run_extract --help

# Extract ubcov_id 15 with full debug output. Please do this **and send the output** if you want to contact <USERNAME> for help
run_extract --debug 15
```

You can also define a `.winnower.ini` in your user directory (`C:/Users/YOURNETID/.winnower.ini` on Windows, `~/.winnower.ini` on OSX/Linux) to **set a default save location**.

```
# .winnower.ini - provides config values by default so you don't have to type them a lot

# This sets the default directory to save files in
# Anything added with the -o flag will be added to this
directory = <FILEPATH>

# This sets the default directory to save files in IFF they have inputs from LIMITED_USE
lu-directory = <FILEPATH>

# This sets the default file name like the '-o' flag. It can create directories
# this saves all extractions in per-iso3 folders using the standarding naming
# the file extension (.dta or .csv) will be added to this
output-file = {iso3}/{survey_name}_{nid}_{survey_module}_{ihme_loc_id}_{year_start}_{year_end}
```

### Customizing extractions

`winnower` now includes an extraction hook system for modifying data extractions in three places:

* Between the **merge** step and the **label** step (`pre_extraction`)
* After topic **custom code** runs (`post_custom`)
* After GBD Subnational Mapping (`post_extraction`)

Users of `ubCov` may be helped by the following analogy:

* `pre_extraction` is like using the **subset** column to run a script
* `post_custom` is for adding survey-specific code, like you might have done in e.g., `wash.do`
* `post_extraction` is for any post-processing you do when running `ubCov`s `run_extract` in a loop

New users should know this:

* Some surveys need additional processing or data clean-up beyond what `winnower` is capable of supporting. This cleanup is best accomplished in `post_extraction`.
* In much rarer instances surveys need to be modified/cleaned **before** regular processing. For example, it may be necessary to fix calendar/date issues before the `demographics` module computes things like `age_year` and `int_date`.


#### Writing a custom extraction

* **First** you must know the NID you want to extract from. Let's say it's 93848
* Create `<FILEPATH>/nid_93848.py` if it doesn't already exist
* Ensure it has these imports

    ```python
    from winnower.extract_hooks import ExtractionHook
    ```

* Create a class that subclasses Extraction hook. It needs to
    1. Define the uniquely identifying fields in an extraction (there are 7 - see below)
    1. Define one or more methods decorated as `pre_extraction`, `post_custom`, and `post_extraction`

    Here is a sample

    ```python
    class FixCalendar(ExtractionHook):  # any valid class name is acceptabe
        # These 7 values should be just like what you enter into the Basic codebook
        SURVEY_NAME = 'WB_LSMS_ISA'
        NID = 93848
        IHME_LOC_ID = 'ETH'
        YEAR_START = 2011
        YEAR_END = 2012
        SURVEY_MODULE = 'HHM'
        FILE_PATH = '<FILEPATH>'

        # Runs after merge, before any indicators are generated
        # this is run for all topics because the calendar should always be correct
        @ExtractionHook.pre_extraction.for_all_topics
        def fix_calendar(self, df):
            # re-code seasonality for survey conducted in first half of year
            # existing codes are in the Ethiopian calendar
            # recode from Ethiopian calendar to Gregorian
            df['int_year'] = 2012
            int_month = self.get_column_name('int_month')
            df[int_month] = df[int_month].replace({5: 1, 6: 2, 7: 3, 8: 4, 9: 5})
            return df  # you MUST return the dataframe

        # Runs immediately after custom code.
        # Extraction-specific code that you would have put in a topic's
        # custom code file (e.g., child_growth_failure.do) should go here.
        # this is run for only the child growth failure topic because "birth_weight"
        # is an indicator specific to child growth failure.
        @ExtractionHook.post_custom.for_topics('child_growth_failure')
        def fix_birth_weight(self, df):
            """
            Convert birth_weight to kg.

            This file encoded the unit (lb/kg) into the birth_weight_card variable.
            """
            mn11a = self.get_column_name('mn11a')
            df.loc[df[mn11a].isin((3, 4)), 'birth_weight'] *= 0.453592
            return df  # you MUST return the dataframe
    ```

`ExtractionHook` subclasses have a few important features:

1. All codebook configuration is available as `self.config`
    * What was the `sex` column again? `self.config['sex']`
    * Note that most values will be in **lists** and not a single value
1. In `post_custom` and `post_extraction` indicators will exist in the data frame (`df`)
    * `df['int_year']` will work
1. In `pre_extraction` indicators **do not yet exist** in the data frame (`df`)
    * There is a helper function for this - `get_column_name`

### Valid substitution parameters for `-o`

* `nid`
* `survey_name` - all punctuation will be replaced with `_`s
* `survey_module`
* `ihme_loc_id`
* `iso3` - the first 3 characters of `ihme_loc_id`
* `year` - identical to `year_start`
* `year_start`
* `year_end`

Some other details:

* This can include directory paths e.g.,  `-o "{iso3}/{survey_name}_{year}_{nid}.csv"`
* The program will attempt to make the directory automatically

## Making `winnower` better

Users are encouraged to make pull requests. You may find the [developing and debugging](docsource/development.md) guide helpful.

If you have an idea for a feature, please let us know! Send <USERNAME> a message with your idea!

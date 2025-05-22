# Microdata Collapse Script

After extracting microdata surveys using the Winnower extraction tool, the next step is to collapse all of the extracted survey data. Collapsing is the process in which all of the extracted microdata is aggregated by the parameters specified in the `config.csv` file. For example, microdata could be collapsed (aggregated) on `age_group_id` and `sex_id`.

However, collapsing can be a lengthy and resource intensive process. Given the size and amount of extracted surveys, the collapse process could potentially take days to complete. To circumvent this problem, this collapse script will properly allocate resources and split up surveys to ensure a more efficient collapse.

In this document, the following will be discussed:

1. How to set up the `config.csv` file
2. How to run the main collapse script
3. Troubleshooting/support

___

## 1) Setting up `config.csv` file

The `config.csv` file is sourced during central computation's collapse function to denote what parameters within the extracted microdata need to be collapsed on. The official documentation on the HUB can be [found here](https://hub.ihme.washington.edu/display/UBCOV/Collapse+Documentation). Please read this before running any collapse code.

The main thing to focus on when setting up the `config.csv` file is in the column `input.root`, make sure all extracted surveys are located within the `input.root` directory that you specify.

*Note: if you extracted data from the L-drive, you will need to create two rows in the `config.csv` file to collapse all of the extracted microdata -- one for the surveys extracted from the J-drive and another row for the surveys extracted from the L-drive. However, when specifying the `output.root` for these rows, they can almost always be written out to the same output directory given that collapsing gets rid of all personal identifiers.* **If you are unsure, it is always best to discuss with your research manager to ensure that you are handling all data from the L-Drive correctly and within the guidelines set by the collaborator who supplied the data to IHME.**

___

## 2) Running `main_collapse_script.R`

The collapse code in this repo is designed to split up extracted surveys that are greater than 10M into smaller files based on sex and age group. The newly split files (along with the extracted files that are less than 10M) are then collapsed in parallel, therefore increasing the efficiency of your collapse.

To initiate the collapse code, all you will need to do is run the code found in `main_collapse_script.R`. To start the collapse, you will need to define the following parameters in `main_collapse_script.R`:

| `parameter` | Description |
| --- | --- |
| `winnower_file_dir` | The location where extracted surveys are located (remember to update this when collapsing files originating from the J-Drive and L-Drive) |
| `collapse_config_path` | The path to where the `config.csv` file is located. By default, it points to the config file found in this repo, which you are able to edit to fit your collapse specifications. |
| `collapse_topic` | The `topic` row's settings you want to use found in `config.csv` |

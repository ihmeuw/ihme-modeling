# CoD Meningitis Splits

This bit of code uses the split_cod_model shared function in order to split meningitis into its subcauses. The split_cod_model.py scripty splits the model and save_cod.py saves the new causes in parallel once the splits are complete.

To use each script, all that should be required is editing the username variable in each (or changing the output directory if you choose). 

Note for those unfamiliar with Python. To launch the split_cod_model.py scripty, first activate the GBD envirnment on the cluster by typing:

"source PATH gbd_env"

Then navigate to the folder you are storing the split_cod_model.py file and type "python split_cod_model.py".  
# Checking Location Mapping

We were not able to confirm the mapping from the locations in the raw data to GBD location ids in the documentation. The code in this directory is for that.

It compares the name of the state in the filepath for each data file against the name of the state that we map to.  But, the name of the state in the filepath is the state of the hospital, and the location that we map to is the state of residence of the patient. So, these two locations can differ.  The code tabulates, for each state of residence, the number of states of hospitals. 

For example, among patients who reside in Rondonia, the most frequent state of the hospital they are admitted to is also Rondonia. 

In fact, this is the case for all states of residence, thus confirming our location map.
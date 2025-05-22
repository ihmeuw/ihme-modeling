# Clinical Noise Reduction
The ClinicalNR module contains all of the code required to apply noise reduction to clinical data.

For additional information and history view the noise reduction hub page [here.](URL)

Marketscan Example Workflow
1. Identify a model group. This is a combination of a bundle_id, estimate_id, sex_id and "group location id", eg 102 for the US. This group location is used for raking.
2. Aggregate subnationals estimates to the national level.
3. Fit a Poisson model to the data at both the subnational and national levels.
4. If 3 converges, store the outputs of the model as a class attribute.
5. If 3 fails, take an average of the available data.
6. Apply Noise reduction using the NR formula. This is an inverse weighting based on a calculation of variance from the model and the data.
7. Rake subnationals to match the national data.
8. Set the estimate floor and set estimates below it to zero. We do not allow noise reduced outputs to be lower than any observed point in the data.

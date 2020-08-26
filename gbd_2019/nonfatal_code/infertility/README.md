
# Infertility pipeline

##### tasks.py

tasks.py is a submitter script that breaks the infertility pipeline into object-oriented classes. Each class has the following three things in common: (1) Parameter lists that are passed to subscripts and that explain how source and target modelable_entities relate to one another, (2) execution of a data manipulation step that performs some type of custom calculation on the input data (parallelized by location_id or year_id where applicable), and (3) execution of a save step that saves the results from (2) to a database, either via save_results_epi() or upload_epi_data(). The Westrom() class combines (2) and (3) into a single script.

The following data manipulation scripts are executed from tasks.py: sev_splits.py, epi_splits_pid.py, calc_env.py, westrom.py, female_attr.py, excess.py, and male_attr.py.

The tasks.py submitter script is capable of submitting one class at a time without the Luigi and Jobmon modules that were used in previous versions. The user controls submission of classes by commenting/uncommenting class instantiations under 'if __name__ == "__main__":'. To execute the script, the user must login to the cluster, activate the gbd environment, navigate to the folder where tasks.py is stored, type 'python tasks.py' into the console, then hit enter. Commenting/uncommenting class instantiation requires knowledge of pipeline order. If all input models are ready, SevSplit(), PID(), and Envelope() can be run concurrently. Westrom() depends on the outputs from PID(). FemaleInfert() depends on the outputs of SevSplit(), Envelope(), and the Dismod models from Westrom(). Excess() depends on the outputs from SevSplit() and FemaleInfert(). MaleInfert() depends on the outputs from SevSplit() and Envelope().

##### sev_splits.py

This code is a modified copy of central computation's sev_split.py.
Given a split_verson_id and an output directory, it splits a single modelable entity (also known as a source model or parent model) into two or more modelable entities (target or child models) according to proportions saved in a database. Proportions are derived from literature reviews on long-term health outcomes and uploaded to the database via request form found.

Infertility-related health outcomes for Klinefelter syndrome, Turner syndrome, Congenital genital anomalies, Endometriosis, and Polycystic ovary syndrome (PCOS) are split out with this script and then used in the female_attr.py, excess.py, and male_attr.py steps of the infertility pipeline.

##### save.py

Saves draws from custom calculations via the save_results_epi() shared function. This is a common file that is used in most steps of the infertility pipeline. If optional arguments are not passed to the save.py file, default values will be used instead.

##### epi_splits_pid.py

Splits the Pelvic inflammatory diseases (PID) parent model into 3 STI etiologies (PID due to chlamydial infection, PID due to gonococcal infection, and PID due to other sexually transmitted diseases) via proportion models. Uses the shared function split_epi_model(). If optional arguments are not passed to the epi_splits_pid.py file, default values will be used instead.

Incidence for the 3 STI etiologies is needed in the westrom.py step of the infertility pipeline and prevalence is needed for final GBD impairment reporting so both measures must be split from the PID parent model.

##### calc_env.py

calc_env.py splits Dismod primary and secondary infertility envelopes into sex-specific, male and female envelopes using proportion and exposure models. See appendix write-up for definitions of primary and secondary infertility.

Using eight Dismod models as inputs, this script outputs data for four custom models. The first four input models are 'Prevalence of primary infertility among exposed (2421)', 'Prevalence of exposure to primary infertility (2794)', 'Prevalence of secondary inferility among exposed (2422)', and 'Prevalence of exposure to secondary infertilty (2795)', Only the female results for these four models are used. The second four input models are 'Proportion female primary infertility (2824)', 'Proportion male primary infertility (2823)', 'Proportion female secondary infertility (2826)', and 'Proportion male secondary infertility (2825)'.

The primary and seconday infertility envelopes and exposure models are based on survey data where women in heterosexual unions were questioned but the infertile person/s in the partnership could not be determined by the questions asked. Thus, the results of the survey can be considered an indicator of couples’ infertility. For this reason, envelope and exposure data are uploaded to the epi database as female data, Dismod models are run, and calc_env.py copies the female results to the male sex_id for further custom calculations.

To estimate the prevalence of infertility among couples, we multiply prevalence among the exposed by the exposure prevalence. Then, we estimate the prevalence of infertility among each sex by multiplying the result from the previous calculation by the appropriate sex-specific proportion model. 

Because infertility in some couples is attributable to both partners rather than just one, the sum of the proportion of couples’ prevalence due to male factor infertility and due to female factor infertility can be greater than 1. If the sum of male factor infertility and female factor infertility is less than 1, calc_env.py scales the proportions so they sum to 1.

##### westrom.py

Calculates prevalence of infertility due to PID from STIs.

Applies a proportion from Westrom et al. (1992, Sex Transm Dis) to incidence for each of the 3 STI etiologies derived in the epi_splits_pid.py step. The result is formatted for upload to the epi database. Old data in bundles 412, 413, and 414 are deleted and new data are uploaded. Researcher is notified once the bundle uploads are complete and Dismod models are run for modelable_entity_ids 3022, 3023 and 3024 using the uploaded data. For GBD2017, the following Dismod parameters were used on modelable_entity_ids 3022, 3023 and 3024: remission = 0, excess mortality = 0.

female_attr.py should not be launched until the Dismod models are complete.

##### female_attr.py

There are eight identified causes of female infertility in the GBD2017 cause list: chlamydia, gonorrhea, other sexually transmitted diseases, maternal sepsis, polycystic ovarian syndrome, endometriosis, Turner syndrome, and congenital genital anomalies.

female_attr.py performs four tasks:

    1. Calculates primary and secondary infertility ratios from the primary and secondary envelopes output from calc_env.py. It is assumed that all infertility from Turner syndrome and congenital genital anomalies is primary infertility so prevalence for these causes is subtracted from the primary infertility envelope before calculating the ratios.

    2. Applies the infertility ratios to six of the eight identified causes of female infertility (chlamydia, gonorrhea, other sexually transmitted diseases, maternal sepsis, polycystic ovarian syndrome, and endometriosis). It is assumed that all infertility following maternal sepsis is secondary infertility so only the secondary infertility ratio is applied to maternal sepsis.

    3. Squeezes prevalence of primary infertility due chlamydia, gonorrhea, other sexually transmitted diseases, maternal sepsis, polycystic ovarian syndrome, and endometriosis to 95% of the primary infertility envelope if the sum of these sub-causes is greater than 95% of the envelope. Assigns 5% of the envelope plus the residual of the sub-cause sum (if the sum is less than 95% of the envelope) to "Idiopathic primary female infertility (2071)". Repeats the process for the secondary envelope and assigns the remaining proportion to "Idiopathic secondary female infertility (2072)".

    4. Sums all of the input models and all of the squeezed output models for endometriosis. Subtracts output sum from input sum and saves the result to 'Excess infertility cases due to endometriosis (9748)'. Where subtraction would result in negative draws, the draws are set to 0. Repeats the same steps for polycystic ovarian syndrome and saves the result to 'Excess infertility cases due to polycystic ovarian syndrome (9743)'.

##### excess.py

Redistributes the excess prevalence generated in female_attr.py among the PCOS and Endo severity split outputs from sev_splits.py and saves the results to new modelable_entity_ids.

Also copies incidence from the PCOS and Endo parents used in sev_splits.py to the new me_ids. The incidence copy step was previously done via the gyne_copy_over code, but, for GBD2017, the gyne_copy_over code was incorporated into the excess redistribution step of the infertility pipeline to reduce the number of draws saved to our system. 

##### male_attr.py

The only recognised causes of male infertility in the GBD 2017 cause list are Klinefelter syndrome and congenital genital anomalies. We assume that all infertility from these causes is primary infertility.

male_attr.py subtracts infertility due to Klinefelter syndrome and infertility due to congenital genital anomalies from the primary male infertility envelope calculated in calc_env.py to obtain primary male infertility due to "other causes". The secondary male infertility envelope calculated in calc_env.py is copied to a new modelable_entity for carry-through to subsequent GBD processes.
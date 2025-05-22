from cms.pre_db_processing.modify_sas_scripts import modify_sas_scripts


def initial_clinical_extractions():
    modify_sas_scripts(years=[2000, 2010, 2014, 2015, 2016], request_id=8140)


def secondary_dex_extraction():
    modify_sas_scripts(years=[2008, 2009, 2011, 2012, 2013, 2017], request_id=10448)

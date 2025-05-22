'''
MDCR:
    STATE_CD:
        map was pulled from:
        https://www.resdac.org/cms-data/variables/state-code-beneficiary-ssa-code

        Note that this is the SSA state location. The location ids are also stored as strings,
        hence why we see values such as 01.
    
    STATE_CNTY_FIPS:
    map was pulled from:
    https://www.census.gov/geographies/reference-files/2015/demo/popest/2015-fips.html

    Note that the state location is the first two digits of the FIPS code. 

MAX:
  map was pulled from:
      https://www.resdac.org/sites/resdac.umn.edu/files/State%20Table_2.txt
'''
import collections

Codes = collections.namedtuple('Codes', ['STATE_CD',       # MDCR
                                        'STATE_CNTY_FIPS', # MDCR
                                        'MAX_STATE_CD'     # MAX
                                        ])
loc_lookup = {
    523: Codes(STATE_CD='01', STATE_CNTY_FIPS='01', MAX_STATE_CD='AL'),
    524: Codes(STATE_CD='02', STATE_CNTY_FIPS='02', MAX_STATE_CD='AK'),
    525: Codes(STATE_CD='03', STATE_CNTY_FIPS='04', MAX_STATE_CD='AZ'),
    526: Codes(STATE_CD='04', STATE_CNTY_FIPS='05', MAX_STATE_CD='AR'),
    527: Codes(STATE_CD='05', STATE_CNTY_FIPS='06', MAX_STATE_CD='CA'),
    528: Codes(STATE_CD='06', STATE_CNTY_FIPS='08', MAX_STATE_CD='CO'),
    529: Codes(STATE_CD='07', STATE_CNTY_FIPS='09', MAX_STATE_CD='CT'),
    530: Codes(STATE_CD='08', STATE_CNTY_FIPS='10', MAX_STATE_CD='DE'),
    531: Codes(STATE_CD='09', STATE_CNTY_FIPS='11', MAX_STATE_CD='DC'),
    532: Codes(STATE_CD='10', STATE_CNTY_FIPS='12', MAX_STATE_CD='FL'),
    533: Codes(STATE_CD='11', STATE_CNTY_FIPS='13', MAX_STATE_CD='GA'),
    534: Codes(STATE_CD='12', STATE_CNTY_FIPS='15', MAX_STATE_CD='HI'),
    535: Codes(STATE_CD='13', STATE_CNTY_FIPS='16', MAX_STATE_CD='ID'),
    536: Codes(STATE_CD='14', STATE_CNTY_FIPS='17', MAX_STATE_CD='IL'),
    537: Codes(STATE_CD='15', STATE_CNTY_FIPS='18', MAX_STATE_CD='IN'),
    538: Codes(STATE_CD='16', STATE_CNTY_FIPS='19', MAX_STATE_CD='IA'),
    539: Codes(STATE_CD='17', STATE_CNTY_FIPS='20', MAX_STATE_CD='KS'),
    540: Codes(STATE_CD='18', STATE_CNTY_FIPS='21', MAX_STATE_CD='KY'),
    541: Codes(STATE_CD='19', STATE_CNTY_FIPS='22', MAX_STATE_CD='LA'),
    542: Codes(STATE_CD='20', STATE_CNTY_FIPS='23', MAX_STATE_CD='ME'),
    543: Codes(STATE_CD='21', STATE_CNTY_FIPS='24', MAX_STATE_CD='MD'),
    544: Codes(STATE_CD='22', STATE_CNTY_FIPS='25', MAX_STATE_CD='MA'),
    545: Codes(STATE_CD='23', STATE_CNTY_FIPS='26', MAX_STATE_CD='MI'),
    546: Codes(STATE_CD='24', STATE_CNTY_FIPS='27', MAX_STATE_CD='MN'),
    547: Codes(STATE_CD='25', STATE_CNTY_FIPS='28', MAX_STATE_CD='MS'),
    548: Codes(STATE_CD='26', STATE_CNTY_FIPS='29', MAX_STATE_CD='MO'),
    549: Codes(STATE_CD='27', STATE_CNTY_FIPS='30', MAX_STATE_CD='MT'),
    550: Codes(STATE_CD='28', STATE_CNTY_FIPS='31', MAX_STATE_CD='NE'),
    551: Codes(STATE_CD='29', STATE_CNTY_FIPS='32', MAX_STATE_CD='NV'),
    552: Codes(STATE_CD='30', STATE_CNTY_FIPS='33', MAX_STATE_CD='NH'),
    553: Codes(STATE_CD='31', STATE_CNTY_FIPS='34', MAX_STATE_CD='NJ'),
    554: Codes(STATE_CD='32', STATE_CNTY_FIPS='35', MAX_STATE_CD='NM'),
    555: Codes(STATE_CD='33', STATE_CNTY_FIPS='36', MAX_STATE_CD='NY'),
    556: Codes(STATE_CD='34', STATE_CNTY_FIPS='37', MAX_STATE_CD='NC'),
    557: Codes(STATE_CD='35', STATE_CNTY_FIPS='38', MAX_STATE_CD='ND'),
    558: Codes(STATE_CD='36', STATE_CNTY_FIPS='39', MAX_STATE_CD='OH'),
    559: Codes(STATE_CD='37', STATE_CNTY_FIPS='40', MAX_STATE_CD='OK'),
    560: Codes(STATE_CD='38', STATE_CNTY_FIPS='41', MAX_STATE_CD='OR'),
    561: Codes(STATE_CD='39', STATE_CNTY_FIPS='42', MAX_STATE_CD='PA'),
    562: Codes(STATE_CD='41', STATE_CNTY_FIPS='44', MAX_STATE_CD='RI'),
    563: Codes(STATE_CD='42', STATE_CNTY_FIPS='45', MAX_STATE_CD='SC'),
    564: Codes(STATE_CD='43', STATE_CNTY_FIPS='46', MAX_STATE_CD='SD'),
    565: Codes(STATE_CD='44', STATE_CNTY_FIPS='47', MAX_STATE_CD='TN'),
    566: Codes(STATE_CD='45', STATE_CNTY_FIPS='48', MAX_STATE_CD='TX'),
    567: Codes(STATE_CD='46', STATE_CNTY_FIPS='49', MAX_STATE_CD='UT'),
    568: Codes(STATE_CD='47', STATE_CNTY_FIPS='50', MAX_STATE_CD='VT'),
    569: Codes(STATE_CD='49', STATE_CNTY_FIPS='51', MAX_STATE_CD='VA'),
    570: Codes(STATE_CD='50', STATE_CNTY_FIPS='53', MAX_STATE_CD='WA'),
    571: Codes(STATE_CD='51', STATE_CNTY_FIPS='54', MAX_STATE_CD='WV'),
    572: Codes(STATE_CD='52', STATE_CNTY_FIPS='55', MAX_STATE_CD='WI'),
    573: Codes(STATE_CD='53', STATE_CNTY_FIPS='56', MAX_STATE_CD='WY'),
    # TERRITORIES
    298: Codes(STATE_CD='99', STATE_CNTY_FIPS='60', MAX_STATE_CD='AS'),
    351: Codes(STATE_CD='98', STATE_CNTY_FIPS='66', MAX_STATE_CD='GU'),
    385: Codes(STATE_CD='40', STATE_CNTY_FIPS='72', MAX_STATE_CD='PR'),
    422: Codes(STATE_CD='48', STATE_CNTY_FIPS='78', MAX_STATE_CD='VI'),
    376: Codes(STATE_CD='97', STATE_CNTY_FIPS='69', MAX_STATE_CD='MP'),

   
}
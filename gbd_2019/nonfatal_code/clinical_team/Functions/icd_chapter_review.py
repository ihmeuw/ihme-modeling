import pandas as pd
import warnings


def get_chapter_dict(code_system_id):
    """Create the mapping between ICD chapters and ICD codes"""

    if code_system_id == 2:
        ch_dict = {  '1: Certain infectious and parasitic diseases': ('A00', 'B99999'),
                     '2: Neoplasms': ('C00', 'D49999'),
                     '3: Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism': ('D50', 'D89999'),
                     '4: Endocrine, nutritional and metabolic diseases': ('E00', 'E99999'),
                     '5: Mental and behavioural disorders': ('F00', 'F99999'),
                     '6: Diseases of the nervous system': ('G00', 'G99999'),
                     '7: Diseases of the eye and adnexa': ('H00', 'H59999'),
                     '8: Diseases of the ear and mastoid process': ('H60', 'H95999'),
                     '9: Diseases of the circulatory system': ('I00', 'I99999'),
                     '10: Diseases of the respiratory system': ('J00', 'J99999'),
                     '11: Diseases of the digestive system': ('K00', 'K95999'),
                     '12: Diseases of the skin and subcutaneous tissue': ('L00', 'L99999'),
                     '13: Diseases of the musculoskeletal system and connective tissue': ('M00', 'M99999'),
                     '14: Diseases of the genitourinary system': ('N00', 'N99999'),
                     '15: Pregnancy, childbirth and the puerperium': ('O00', 'O99999'),
                     '16: Certain conditions originating in the perinatal period': ('P00', 'P96999'),
                     '17: Congenital malformations, deformations and chromosomal abnormalities': ('Q00', 'Q99999'),
                     '18: Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified': ('R00', 'R99999'),
                     '19: Injury, poisoning and certain other consequences of external causes': ('S00', 'T98999'),
                     '20: External causes of morbidity and mortality': ('V01', 'Y98999'),
                     '21: Factors influencing health status and contact with health services': ('Z00', 'Z99999'),
                     '22: Codes for special purposes': ('U00', 'U99999')}

    elif code_system_id == 1:
        ch_dict = {1: ('001', '139999'),
                   2: ('140', '239999'),
                   3: ('240', '279999'),
                   4: ('280', '289999'),
                   5: ('290', '319999'),
                   6: ('320', '389999'),
                   7: ('390', '459999'),
                   8: ('460', '519999'),
                   9: ('520', '579999'),
                   10: ('580', '629999'),
                   11: ('630', '679999'),
                   12: ('680', '709999'),
                   13: ('710', '739999'),
                   14: ('740', '759999'),
                   15: ('760', '779999'),
                   16: ('780', '799999'),
                   17: ('800', '999999'),
                   18: ('E000', 'E99999'),
                   19: ('V00', 'V99999')}

    else:
        assert False, "code system {} isn't recognized".format(code_system_id)

    return ch_dict

def test_admission_completeness(df, code_system_id,
                                min_ratio,
                                combine_e_n_codes,
                                agg_type,
                                trim_cause_code):
    """
    Params:
        df: (pd.DataFrame) raw hospital data with ICD codes present
        code_system_id: (int) clinical standard
        min_ratio: (float) work-in-progress, the code returns a df for chapters below a
                           minimum coding threshold (ie .001)
        combine_e_n_codes: (bool) external and nature of injury cause codes are closely related
                                  some sources will only map to one or the other
        agg_type: (str) acceptable values are 'chapter_start', 'icd_chapter'
        trim_cause_code: (bool) should the cause code column be trimmed to first 3 digits?

    """
    assert code_system_id in [1, 2], "code system {} not recognized".format(code_system_id)
    assert agg_type in ['chapter_start', 'icd_chapter'], "can't aggregate to {}".format(agg_type)

    ch_dict = get_chapter_dict(code_system_id)

    df = df.query("diagnosis_id == 1 & code_system_id == @code_system_id")
    pre = len(df)
    df = df[~df['cause_code'].str.contains("^U")]
    warnings.warn("dropping {} rows because they start with the special chapter 'U'".format(pre-len(df)))
    if trim_cause_code:
        df['cause_code'] = df['cause_code'].str[0:3]

    if agg_type == 'chapter_start':
        df[agg_type] = df['cause_code'].str[0:1]
    elif agg_type == 'icd_chapter':
        df[agg_type] = None
        for key, value in ch_dict.items():
            df.loc[(df['cause_code'] >= value[0]) &\
                   (df['cause_code'] <= value[1]), 'icd_chapter'] = key
        if df[agg_type].isnull().any():
            assert False, "These cause codes {} don't map to a chapter".format(df.loc[df[agg_type].isnull(), 'cause_code'].unique())

    if combine_e_n_codes:
        if agg_type == 'icd_chapter':

            df.loc[df[agg_type] == 20, agg_type] = 19
        elif agg_type == 'chapter_start':

            df.loc[df[agg_type].isin(['V', 'W', 'X', 'Y']), agg_type] = 'S'
    else:
        pass

    ch_list = []
    for n in df.nid.unique():
        tmp = df[df.nid == n]
        ch = tmp.groupby([agg_type]).agg({'val': 'sum'}).reset_index()
        ch['nid'] = n

        ch['all_admits'] = tmp.val.sum()

        ch['{}_ratio'.format(agg_type)] = ch['val'] / ch['all_admits']

        ch_list.append(ch)

    ch = pd.concat(ch_list, ignore_index=True)
    ch = ch[['nid', agg_type, 'val', 'all_admits', '{}_ratio'.format(agg_type)]]

    sorted_index = ch['icd_chapter'].str.extract("(\d+)").astype(int).sort_values(0).index

    if code_system_id == 2:
        present_chapts = ch['icd_chapter'].unique()
        for key in ch_dict.keys():
            if key[0:2] == '20' and '19' in [c[0:2] for c in ch['icd_chapter'].unique()]:
                pass
            elif key[0:2] == '22':
                pass
            else:
                assert key in present_chapts, "{} is missing!!".format(key)
        assert ch.icd_chapter.unique().size >= 20, "There aren't a full range of ICD codes present"
    if code_system_id == 1:
        assert False, 'still working'

    below_min = ch[ch['{}_ratio'.format(agg_type)] < min_ratio]
    if len(below_min) > 0:
        print("Returning only the rows that are below the min threshold")
        return below_min


    print("Returning all rows")

    return ch.reindex(sorted_index)


if __name__ == '__main__':


    source_path = """FILEPATH"""
    df = pd.read_hdf(source_path)

    x = test_admission_completeness(df=df.copy(),
                                    trim_cause_code=True,
                                    code_system_id=2,
                                    min_ratio=.001,
                                    combine_e_n_codes=False,
                                    agg_type='icd_chapter')

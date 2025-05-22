import collections

Codes = collections.namedtuple('Codes', ['SEX_IDENT_CD', # MDCR
                                        'EL_SEX_CD'      # MAX
                                        ])

sex_lookup = {
    1 : Codes(SEX_IDENT_CD=1, EL_SEX_CD='M'),
    2 : Codes(SEX_IDENT_CD=2, EL_SEX_CD='F'),
    4 : Codes(SEX_IDENT_CD=0, EL_SEX_CD='U')
}
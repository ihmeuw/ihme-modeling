from decorator import decorator
from unidecode import unidecode


@decorator
def convert_to_ascii(func, *args, **kwargs):
    df = func(*args, **kwargs)
    return _convert_to_ascii(df)


def _convert_to_ascii(df):
    for col in df:
        try:
            df[col].str.decode('ascii')
        except UnicodeEncodeError:
            df[col] = df[col].apply(
                lambda val: unidecode(val) if isinstance(val,
                                                         basestring) else val)
        except AttributeError:
            pass
    return df

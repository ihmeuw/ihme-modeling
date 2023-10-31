from winnower.util.dtype import is_string_dtype


class UnicodeSimplifierBase:
    """
    Class for simplifying unicode to ascii values.

    Users can instantiate this class if they desire, but a convenience global
    is provided below as UnicodeSimplifier. It is suggested to use this.
    """
    # thanks to <USERNAME> for putting together this excellent list
    diacritic_trans = {'Š': 'S', 'š': 's', 'Ž': 'Z', 'ž': 'z', 'À': 'A',
                       'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A', 'Å': 'A',
                       'Æ': 'A', 'Ç': 'C', 'È': 'E', 'É': 'E', 'Ê': 'E',
                       'Ë': 'E', 'Ì': 'I', 'Í': 'I', 'Î': 'I', 'Ï': 'I',
                       'Ñ': 'N', 'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O',
                       'Ö': 'O', 'Ø': 'O', 'Ù': 'U', 'Ú': 'U', 'Û': 'U',
                       'Ü': 'U', 'Ý': 'Y', 'Þ': 'B', 'à': 'a', 'á': 'a',
                       'â': 'a', 'ã': 'a', 'ä': 'a', 'å': 'a', 'æ': 'a',
                       'ç': 'c', 'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                       'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i', 'ð': 'o',
                       'ñ': 'n', 'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
                       'ö': 'o', 'ø': 'o', 'ù': 'u', 'ú': 'u', 'û': 'u',
                       'ý': 'y', 'ý': 'y', 'þ': 'b', 'ÿ': 'y',
                       # Eszett character. Per <USERNAME> "ss" is right translation
                       'ß': 'ss',
                       }
    table = str.maketrans(diacritic_trans)

    def convert_unicode_characters_to_ascii(self, df):
        """
        Translates diacritics characters and removes non-ascii characters.
        """
        columns = filter(is_string_dtype, (df[col] for col in df))
        for column in columns:
            # translate diacritics
            column = column.str.translate(self.table)
            # strip non-ascii characters (removes data)
            column = (column
                    .str.encode("latin-1", errors="ignore")
                    .str.decode('utf-8'))
            df[column.name] = column

        return df


UnicodeSimplifier = UnicodeSimplifierBase()

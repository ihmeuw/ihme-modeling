# FROM ICD group ID constants
INIT_MAP_VERSION: int = 30

# add some new ICD 10 codes that are known to not match patterns above
KNOWN_DIFF = ["C7A", "C7B", "D3A", "M1A", "O9A", "Q57", "Z3A"]

TRUNCATION_LEN = 3
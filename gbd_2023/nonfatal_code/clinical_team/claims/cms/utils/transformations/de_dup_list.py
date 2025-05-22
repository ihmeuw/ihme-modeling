'''
Remove duplicate elements in list while persevering 
order. 

Useful for columns where there there is an entry
for each month (eg. FIPS codes)
'''

def de_dup_list_vals(seq):
    seen = set()
    seen_add = seen.add
    return [x 
            for x in seq 
            if not (x in seen or seen_add(x))
            ]

def de_dup_fips(seq):
    seen = set()
    seen_add = seen.add
    return [str(int(x)).zfill(5) 
            for x in seq 
            if not (x in seen or x ==-1 or seen_add(x))
            ]
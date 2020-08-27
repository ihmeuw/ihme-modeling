def validate_pct_change_years(year_start_ids, year_end_ids):
    failed = False
    if year_start_ids and year_end_ids:
        if not len(year_start_ids) == len(year_end_ids):
            failed = True
        else:
            if not all(year_start_ids[i] < year_end_ids[i] for i
                       in range(len(year_start_ids))):
                failed = True
    else:
        if year_start_ids or year_end_ids:
            failed = True

    if failed:
        raise ValueError(
            "'year_start_ids' and 'year_end_ids' have a "
            "1 to 1 correspondence and thus must be the "
            "same length. Additionally, each value "
            "in 'year_start_ids' must be strictly less than "
            "the corresponding value in 'year_end_ids'. "
            "Passed: year_start_ids = {ysids}, "
            "year_end_ids = {yeids}".format(ysids=year_start_ids,
                                            yeids=year_end_ids))

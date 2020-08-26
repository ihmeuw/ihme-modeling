from RegressionModifier import DirectRegressionModifier


if __name__ == "__main__":
    version_id = 2562
    braz_mod = DirectRegressionModifier(
        version_id, "2017_05_04_brazil_rti_direct_2",
        verbose=False
        )
    braz_mod.generate_new_proportions()
    # wait til this version id is copied before running
    assert version_id != 2378
    braz_mod.update_proportions_in_engine_room()

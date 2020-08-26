from RegressionModifier import ZeroOutRegressionModifier

if __name__ == "__main__":
    work_folder = "2017_05_04_left_hf_zero_resp_pneum_other"
    mod = ZeroOutRegressionModifier(2560, work_folder)
    mod.set_zero_acause("resp_pneum_other")
    mod.generate_new_proportions()
    # mod.update_proportions_in_engine_room()

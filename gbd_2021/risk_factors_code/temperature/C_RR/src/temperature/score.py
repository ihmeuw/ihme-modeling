import numpy as np
import pandas as pd

import process
import utils
import matplotlib.pyplot as plt


def scorelator(df, trend_result, tdata, outcome, path_to_result_folder, n_samples=10000):
    """Estimate the score for each annual mean temperature.
       The score is estimated as the area between lower bound and the x-axis.
    """
    col_draws = ['draw_{}'.format(i) for i in np.arange(n_samples)]
    score_dict = {}
    # Loop over annual temperature
    for annual_temp in df.annual_temperature.unique():
        # Get draws
        draws = df.loc[df.annual_temperature==annual_temp, col_draws]
        dt = df.loc[df.annual_temperature==annual_temp, 'daily_temperature']
        # Return the index of row that corresponds to the minimum mean draw
        # In other words, the row of the daily mean temperature that has the lowest mean value
        mean_draws = draws.mean(axis=1)
        min_index = np.argmin(mean_draws)
        # Shift the draws by getting the difference of draws and minimum mean draws
        min_draws = draws.iloc[min_index]
        shifted_draws = draws - min_draws
        # Lower bound for draws across daily mean temperature
        draws_lb = np.quantile(shifted_draws, 0.05, axis=1)
        # Return the score by estimating the area between lower bound and x-axis
        score = np.mean(draws_lb)
        score_dict[annual_temp] = np.round(score, 4)
        plot_score(trend_result, tdata, annual_temp, outcome, score, 
            dt, shifted_draws, mean_draws, path_to_result_folder)
    df_score = pd.DataFrame(list(score_dict.items()),columns = ['annual_temperature', 'score'])    
    return df_score


def plot_score(trend_result, tdata, mean_temp, outcome, score, 
    dt, shifted_draws, mean_draws, path_to_result_folder):
    """Plot the evidence score and shifted draws."""
    tdata_amt = process.extract_at_mean_temp(tdata, mean_temp)
    study_slices = utils.sizes_to_slices(tdata_amt.study_sizes)
    study_range = range(0, tdata_amt.num_studies)
    
    plt.figure(figsize=((16,8)))
    # plot all the points
    for i in study_range:
        s = study_slices[i]
        plt.scatter(tdata_amt.daily_temp[s],
                    tdata_amt.obs_mean[s] - np.min(mean_draws),
                    s=1.0/tdata_amt.obs_std[s])
    # Trimmed data points
    trimming_id = tdata_amt.trimming_weights <= 0.5
    plt.scatter(tdata_amt.daily_temp[trimming_id],
                   tdata_amt.obs_mean[trimming_id],
                   marker='x',
                   color='r')
    # Upper and lower bound
    plt.fill_between(dt, 
                     np.quantile(shifted_draws, 0.05, axis=1), 
                     np.quantile(shifted_draws, 0.95, axis=1),
                     color='#808080', alpha=0.7)
    # Mean draws after shifted
    plt.plot(dt, np.mean(shifted_draws, axis=1), color='b')
    plt.plot([dt.min(), dt.max()], [0.0, 0.0], "k--")
    plt.xlabel("Daily temperature")
    plt.title(f"Mean temperature at {mean_temp}, score: {np.round(score, 4)}")
    plt.savefig(path_to_result_folder + "/" + outcome + "_score_%i.pdf" % mean_temp,
                bbox_inches="tight")
    plt.close()

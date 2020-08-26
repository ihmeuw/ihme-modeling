def rescale_qx_conditional(t):
    # Convert single-year qx to conditional probability space
    # Given that they will die between 1 and 4 what is the probability they will die during this age group
    t['c_1q1'] = t['qx_1'] / t['q_ch']
    t['c_1q2'] = (1 - t['qx_1']) * t['qx_2'] / t['q_ch']
    t['c_1q3'] = (1 - t['qx_1']) * (1 - t['qx_2']) * t['qx_3'] / t['q_ch']
    t['c_1q4'] = (1 - t['qx_1']) * (1 - t['qx_2']) * (1 - t['qx_3']) * t['qx_4'] / t['q_ch']

    # Create 4q1 from conditional qx values
    t['c_4q1'] = t['c_1q1'] + t['c_1q2'] + t['c_1q3'] + t['c_1q4']
    t['scalar'] = t['q_ch'] / t['c_4q1']

    # Generate scaling factor
    t['qx_1'] = (t['c_1q1'] / t['c_4q1']) * t['q_ch']
    t['qx_2'] = t['c_1q2'] * (t['q_ch'] / t['c_4q1']) * (1 / ((1 - t['qx_1'])))
    t['qx_3'] = t['c_1q3'] * (t['q_ch'] / t['c_4q1']) * (1 / ((1 - t['qx_1']) * (1 - t['qx_2'])))
    t['qx_4'] = t['c_1q4'] * (t['q_ch'] / t['c_4q1']) * (1 / ((1 - t['qx_1']) * (1 - t['qx_2']) * (1 - t['qx_3'])))

    return t
  
  
def rescale_qx_iterator(t, iterations, number_of_scalars, scalar_max, scalar_min):
    # Calculate 4q1 based off of individual qx values for 1-4
    t['pv4q1'] = 1 - (1 - t['qx_1']) * (1 - t['qx_2']) * (1 - t['qx_3']) * (1 - t['qx_4'])
  
    # Set scalar min and max as columns in the data
    t['scalar_max'] = scalar_max
    t['scalar_min'] = scalar_min
  
    # Begin iterator
    for x in range(iterations):
        diff_columns = []
        for cq in range(number_of_scalars + 1):
            cq_col = "y{}".format(cq)
            rq_col = "r{}".format(cq + 1)
            t[cq_col] = t['scalar_min'] + cq * ((t['scalar_max'] - t['scalar_min']) / float(number_of_scalars))
            t[rq_col] = t[cq_col]
  
            adjusted_col = "adjusted_4q1_{}".format(cq + 1)
  
            t[adjusted_col] = 1 - (1 - t['qx_1'] * (1 + t[rq_col])) * (1 - t['qx_2'] * (1 + t[rq_col])) * (1 - t['qx_3'] * (1 + t[rq_col])) * (1 - t['qx_4'] * (1 + t[rq_col]))
            
            diff_col = "diff_4q1_{}".format(cq + 1)
            diff_columns.append(diff_col)
            t.loc[t[adjusted_col] != 0, diff_col] = abs(t.loc[t[adjusted_col] != 0, adjusted_col] - t.loc[t[adjusted_col] != 0, "q_ch"])
  
        t['diff_min'] = t[diff_columns].min(axis=1)
  
        for cq in range(number_of_scalars + 1):
            rq_col = "r{}".format(cq + 1)
            diff_col = "diff_4q1_{}".format(cq + 1)
        
            t.loc[t[diff_col] == t['diff_min'], 'r_min'] = t.loc[t[diff_col] == t['diff_min'], rq_col]
  
  
        t['scalar_adjustment'] = abs(t['scalar_max'] - t['scalar_min']) / float(number_of_scalars)
        t['scalar_max'] = t['r_min'] + 2 * t['scalar_adjustment']
        t['scalar_min'] = t['r_min'] - 2 * t['scalar_adjustment']
        t.loc[t['scalar_min'] < 0, 'scalar_min']  = 0.000000001
  
    return t
  
  
def convert_qx_to_days(data_series, days):
    return 1 - (1 - data_series)**(1.0/float(days))

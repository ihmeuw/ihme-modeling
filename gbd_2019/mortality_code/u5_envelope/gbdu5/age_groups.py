class MortAgeGroup(object):
  def __init__(self, name, days, start_day):
  self.name = name
  self.days = days
  self.start_day = start_day
  
  @property
  def end_day(self):
    return float(self.start_day) + float(self.days)
  
  @property
  def years(self):
    return float(self.days)/365.0
  
  @property
  def start_year(self):
    return float(self.start_day)/365.0
  
  @property
  def end_year(self):
    return float(self.end_day)/365.0
  
  
  def __str__(self):
    return self.name
  
  def __repr__(self):
    return self.name
  
  def bin_five_year_ages(value):
    bins = [0, 1]
    bins += [x for x in range(5, 120, 5)]
    start_end_list = [(bins[x-1], bins[x]) for x in range(1, len(bins))]
    for s, e in start_end_list:
      if s <= value < e:
        return s
  
  def btime_to_wk(btime):
    year = int(btime)
    return int((btime - year)/(1/52.))
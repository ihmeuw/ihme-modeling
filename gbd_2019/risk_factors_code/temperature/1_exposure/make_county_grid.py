import pandas as pd
import numpy as np

import fiona

import shapely
from shapely.geometry import Polygon, Point, shape, MultiPoint
from shapely.prepared import prep

shapes = USERNAME.open('FILEPATH')

def grid_points(shape):
    bds = shape.bounds
    i = 24

    lon_min = (bds[0] * i   )  // 1 / i
    lon_max = (bds[2] * i + 1) // 1 / i
    lat_min = (bds[1] * i   )  // 1 / i
    lat_max = (bds[3] * i + 1) // 1 / i
    
    grid = []
    
    for lon in np.arange(lon_min,lon_max,1/i):
        for lat in np.arange(lat_min,lat_max,1/i):
            grid.append(Point(lon,lat))
    grid = MultiPoint(grid)
    
    point_list = list(grid.intersection(shape))
    
    return point_list
    
def truncate(n):
    return n * 100 // 1 / 100

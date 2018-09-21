"""This module contains the Filepaths class."""

class Filepaths:
    """
    This class contains filepaths useful for working within the GBD 
    infrastructure.
    """
    
    def __init__(self):
        
        from os import name
        from os.path import join
        
        if name == 'posix':
            self.j = 'FILEPATH'
        else:
            self.j = 'FILEPATH'
        
        self.work = join(self.j,"FILEPATH")
        
        # High-level directories
        self.dim_dir = join(self.work,"FILEPATH")
        self.epi = join(self.work,"FILEPATH")
        
        # Dimensions
        self.schema = join(self.dim_dir,"FILEPATH")
        self.dimensions = join(self.schema,"dimensions.xlsx")
        
        # Epi
        self.dw_file = join(self.epi,"FILEPATH","FILEPATH","FILEPATH","FILEPATH","dw.csv")
        
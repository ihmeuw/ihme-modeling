

class ComputationElement:
    """A computation element is a function that computes a new dataframe from a
    set of incoming dataframes. This is a Coloring interface."""

    def __init__(self):
        """Does nothing"""

    def get_data_frame(self):
        "Actually do the computation and return a pandas DataFrame."
        return "Undefined virtual method"

    def get_input_indexes(self):
        "List the indexes that are required in the input Data Frames"
        return []

import json

class Config5q0Submodel(object):
    def __init__(self, submodel_id, input_location_ids, output_location_ids):
        self.submodel_id = submodel_id
        self.input_location_ids = input_location_ids
        self.output_location_ids = output_location_ids

    @staticmethod
    def from_dict(data):
        submodel_id = int(data['submodel_id'])
        input_location_ids = [int(x) for x in data['input_location_ids']]
        output_location_ids = [int(x) for x in data['output_location_ids']]
        return Config5q0Submodel(submodel_id, input_location_ids,
                                 output_location_ids)

    def to_dict(self):
        data = {
            "submodel_id": self.submodel_id,
            "input_location_ids": self.input_location_ids,
            "output_location_ids": self.output_location_ids
        }
        return data


class Config5q0RakingModel(object):

    VALID_DIRECTIONS = ['scale', 'aggregate']

    def __init__(self, raking_id, parent_id, child_ids, direction):
        self.raking_id = raking_id
        self.parent_id = parent_id
        self.child_ids = child_ids
        self.direction = direction

        self.is_valid_direction()

    @staticmethod
    def from_dict(data):
        raking_id = int(data['raking_id'])
        parent_id = int(data['parent_id'])
        child_ids = [int(x) for x in data['child_ids']]
        direction = data['direction']
        return Config5q0RakingModel(raking_id, parent_id, child_ids, direction)

    def to_dict(self):
        data = {
            "raking_id": self.raking_id,
            "parent_id": self.parent_id,
            "child_ids": self.child_ids,
            "direction": self.direction
        }
        return data

    def is_valid_direction(self):
        if self.direction not in self.VALID_DIRECTIONS:
            msg = 'direction listed as: {} for raking model {}. Must be {}'.format(
                self.direction, self.raking_id, ', '.join(self.VALID_DIRECTIONS))
            raise AssertionError(msg)


class Config5q0(object):
    def __init__(self, version_id, parent_output_dir="FILEPATH",
                 start_year=1950, end_year=2017, gbd_round_id=5, submodels=[],
                 rakings=[]):
        self.version_id = version_id
        self.parent_output_dir = parent_output_dir
        self.start_year = start_year
        self.end_year = end_year
        self.gbd_round_id = gbd_round_id
        self.submodels = submodels
        self.rakings = rakings

    @property
    def output_dir(self):
        return "{}/{}".format(self.parent_output_dir, self.version_id)

    @staticmethod
    def from_dict(data):
        # Extract version level data
        version_id = int(data['version_id'])
        parent_output_dir = data['parent_output_dir']
        start_year = int(data['start_year'])
        end_year = int(data['end_year'])
        gbd_round_id = int(data['gbd_round_id'])
        # Extract submodels
        submodels = []
        for s in data['submodels']:
            submodels.append(Config5q0Submodel.from_dict(s))
        # Extract raking models
        rakings = []
        for r in data['rakings']:
            rakings.append(Config5q0RakingModel.from_dict(r))
        # Return Config5q0 object
        return Config5q0(version_id, parent_output_dir=parent_output_dir,
                         start_year=start_year, end_year=end_year,
                         gbd_round_id=gbd_round_id, submodels=submodels,
                         rakings=rakings)

    def is_submodel_outputs_unique(self):
        """Makes sure there is no overlap in submodel outputs"""
        # Start with empty containers for all outputs and bad outputs
        all_outputs = []
        bad_outputs = []
        # Loop over submodels, log if there are overlaps
        for s in self.submodels:
            for location_id in s.output_location_ids:
                if location_id in all_outputs:
                    bad_outputs.append(location_id)
                else:
                    all_outputs.append(location_id)
        # Fail if there are overlaps
        if bad_outputs:
            msg = ["These location_ids appear in multiple submodel outputs:"]
            for location_id in bad_outputs:
                bad_submodels = []
                for s in self.submodels:
                    if location_id in s.output_location_ids:
                        bad_submodels.append(s.submodel_id)
                loc_msg = "location_id {}, submodels: {}".format(
                    location_id, ", ".join([str(x) for x in bad_submodels]))
                msg.append(loc_msg)
            raise ValueError("\n".join(msg))

    @staticmethod
    def from_json(file_path):
        with open(file_path) as input_file:
            data = json.load(input_file)
        return Config5q0.from_dict(data)

    def to_dict(self):
        data = {
            "version_id": self.version_id,
            "parent_output_dir": self.parent_output_dir,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "gbd_round_id": self.gbd_round_id,
            "submodels": [s.to_dict() for s in self.submodels],
            "rakings": [r.to_dict() for r in self.rakings]
        }
        return data

    def to_json(self, file_path):
        with open(file_path, 'w') as output_file:
            json.dump(self.to_dict(), output_file)

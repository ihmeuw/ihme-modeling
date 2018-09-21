import dalynator.get_input_args as get_input_args
from dalynator.DalynatorJobSwarm import DalynatorJobSwarm

args = get_input_args.construct_args_run_all_burdenator()
swarm = DalynatorJobSwarm(args)
swarm.run("remote_run_pipeline_burdenator.py")

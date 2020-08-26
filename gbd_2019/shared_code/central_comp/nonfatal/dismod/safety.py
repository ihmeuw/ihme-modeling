import argparse
import os
from typing import Dict, List
import warnings

from db_tools.ezfuncs import query
import pandas as pd
 
from cascade_ode.job_stats import root_dir

DEFAULT_FACTOR = 1.6

# Set default file mask to readable-for all users
os.umask(0o0002)

docstring = f'''
    safety.py edits PATH
    given either a filepath to a csv with a 'modelable_entity_id' column or a
    set of modelable_entity_ids. If no runtimes are provided, rt_factor and 
    mem_factor are increased by DEFAULT_FACTOR: {DEFAULT_FACTOR}.

    Args (all optional):
        --me_ids (int): modelable_entity_ids to add to safety_factors
        -mem, --mem_factor : memory quotient to modify standard dismod
            cascade job resource allocation to.
        -rt, --rt_factor : runtime quotient to modify standard dismod
            cascade job resource allocation to.
        --filepath (str): valid filepath to a .csv file with a column of integers
            corresponding to modelable_entity_ids to add (or modify) in 
            safety_factors.csv
    
    Outputs:
        PATH.csv:
            the modified version of the safety_factors.csv file in the same
            directory. to 'commit' the file, do a:
                mv generated_safety_factors.csv safety_factors.csv
    '''


def parse_arguments() -> None:
    parser = argparse.ArgumentParser(
        description=(docstring), argument_default=argparse.SUPPRESS)
    group =  parser.add_mutually_exclusive_group()
    group.add_argument('--mv-ids', help=f'1 or more model_version_ids to '
                       f'update associated me_id runtime stats for given '
                       f'factors (or default factor {DEFAULT_FACTOR}) to.',
                       type=int, nargs='*', default=None)
    group.add_argument('--me-ids', help=f'1 or more modelable_entity_ids to '
                       f'update runtime stats for given factors (or default '
                       f'factor {DEFAULT_FACTOR}) to.', type=int, nargs='*',
                       default=None)
    group.add_argument('--filepath', help='filepath of csv of me_ids',
                       type=str, default=None)
    parser.add_argument('-mem', '--mem-factor',
                        help='memory modification quotient', 
                        default=DEFAULT_FACTOR, type=float)
    parser.add_argument('-rt', '--rt-factor', help='h_rt modification quotient',
                        type=float, default=DEFAULT_FACTOR)
    args = parser.parse_args()
    return args


def set_factors_for_given_me_ids(
    df: pd.DataFrame,
    me_ids: List[int],
    factor_val: Dict[str, float]
) -> pd.DataFrame:
    # returns a df with all me_ids already present in df plus new me_ids given.
    # foo_factor rows for given me_ids assgined factor_val key:values.
    to_add = pd.DataFrame({'modelable_entity_id': me_ids}).assign(**factor_val)
    to_save = df[~df.modelable_entity_id.isin(me_ids)].append(to_add)

    return to_save


def get_me_id_list_from_mvids(mv_ids: List[int]) -> List[int]:
    qry = '''
        SELECT DISTINCT(modelable_entity_id) FROM epi.model_version
        WHERE model_version_id IN :model_version_ids
    '''
    me_id_list = query(
        qry, parameters={'model_version_ids': mv_ids}, conn_def='epi')
    if not len(me_id_list):
        raise RuntimeError(f"no me_ids for given mv_ids")
    return me_id_list.modelable_entity_id.tolist()


def main() -> None:
    args = parse_arguments()
    # import pdb; pdb.set_trace()
    print(f"vars:\nme_ids={args.me_ids}\nmem_factor={args.mem_factor}\n"
          f"runtime_factor={args.rt_factor}\nfilepath={args.filepath}")

    # Validate and gather inputs
    if args.mv_ids:
        me_id_list = get_me_id_list_from_mvids(args.mv_ids)
    elif args.filepath:
        if not os.path.exists(args.filepath):
            raise FileNotFoundError(f"given filepath {args.filepath} DNE")
        else:
            me_id_list = pd.read_csv(args.filepath).modelable_entity_id.tolist()
            if not all(isinstance(x, int) for x in me_id_list):
                raise ValueError(f'there are vals in the file that are not of '
                                 f'type int. list from csv: \n{me_id_list}')
    elif args.me_ids:
        me_ids = args.me_ids
        if type(me_ids) == int:
            me_ids = [me_ids]
        me_id_list = me_ids
    else:
        raise ValueError("must supply one of: mv_ids, me_ids, or filepath")
    
    factor_val = {
        'factor': min(args.mem_factor, args.rt_factor),
        'mem_factor': args.mem_factor,
        'rt_factor': args.rt_factor
    }

    df = pd.read_csv(os.path.join(root_dir, 'safety_factors.csv'))
    to_save = set_factors_for_given_me_ids(df, me_id_list, factor_val)
    
    print(f"original df len {len(df)}")
    print(f"new df len {len(to_save)}")
    to_save.to_csv(os.path.join(root_dir, 'generated_safety_factors.csv'),
                   index=False)


if __name__ == "__main__":
    main()

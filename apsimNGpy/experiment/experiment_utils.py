import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import numpy as np
from sqlalchemy import create_engine
from apsimNGpy.replacements.replacements import Replacements


################################################################################
# MetaInfo
################################################################################
@dataclass
class MetaInfo:
    """
    Meta info provides meta_infor for each experiment. Here it is initialized with defaults, They can be changed via
    the set experiment method
    """
    verbose: bool = None
    simulations: bool = None
    clear: bool = None
    test: bool = None
    mult_threads: bool = True
    ...


################################################################################
# get_file_path
################################################################################
def get_file_path(iid, path, tag):
    file_pathPattern = f"*_{tag}_{iid}.apsimx"
    fp = list(Path(path).glob(file_pathPattern))
    if bool(fp):
        return fp[0]
    else:
        raise ValueError(f"aPSim file path associated with ID '{iid}' does not exists")


################################################################################
# run experiment
################################################################################

def _run_experiment(*, meta_info, SID, parameters):
    """

    :param meta_info: generated by DesignExperiment class
    :param objs: single objects generated by set experiment
    :return: None
    """
    data_base_CONN = create_engine(f'sqlite:///{meta_info.datastorage}')
    idi = SID

    mgt_parameters = parameters.merge()
    mgt_df = parameters.control_variables(mgt_parameters)
    mgt_df['SID'] = idi
    mf = mgt_df.loc[:, ~mgt_df.columns.duplicated()].copy()

    mf.rename(columns={'Name': 'ManagerName'}, inplace=True)

    fModel = get_file_path(idi, os.path.join(meta_info.path, 'apSimNGpy_experiment'), meta_info.tag)
    Model = Replacements(fModel)
    manager_params = mgt_parameters.get('management')
    if manager_params is not None:
        print(manager_params) if meta_info.test else None
        [Model.update_mgt_by_path(**i) for i in manager_params]
    soil_params = mgt_parameters.get('soils_params')
    if soil_params:
        [Model.replace_soil_properties_by_path(**sp) for sp in soil_params]
    report_tables = meta_info.reports
    Model.run(simulations=meta_info.simulations, multithread=meta_info.mult_threads, get_dict=True)
    res = {tb: Model.results.get(tb) for tb in report_tables if tb in Model.results.keys()}
    for table, data in res.items():
        df = data

        df_no_dupS = df.loc[:, ~df.columns.duplicated()].copy()

        df_no_dupS[meta_info.simulation_id] = idi
        for i in mf.columns:
            if i in df_no_dupS.columns and i != meta_info.simulation_id:
                df_no_dupS.rename(columns={i: f"sim_{i}"}, inplace=True)
        ans = df_no_dupS.merge(mf, on=meta_info.simulation_id, how='inner').reset_index()
        ans_nodup = ans.loc[:, ~ans.columns.duplicated()].copy()
        if not meta_info.test:
            ans_nodup.to_sql(table, con=data_base_CONN, if_exists='append', index=False)
        else:
            print(ans_nodup)
            return ans_nodup
    if meta_info.verbose:
        print(f"\n '{idi}' : completed", end='\r')
    ...


def run_wrapper(data):
    _run_experiment(**data)


################################################################################
# copy files
################################################################################
def copy_to_many(ids, base_file, path, tag):
    # def _copy(i_d):
    _path = Path(path)
    iN = ids
    new_file_name = f"f_{tag}_{iN}.apsimx"
    new_file = os.path.realpath(_path.joinpath(new_file_name))
    if not os.path.isfile(new_file):
        shutil.copy(base_file, new_file)


@dataclass
class Factor:
    """
    placeholder for factor variable and names. it is intended to ease the readability of the factor for the user
    """
    variables: Union[list, tuple, np.ndarray]
    variable_name: str


def define_factor(parameter: str, param_values: list, factor_type: str,
                  manager_name: str = None, soil_node: str = None,
                  simulations: list = None, out_path_name: str = None,
                  crop: str = None, indices: list = None) -> Factor:
    """
    Define a management or soil factor based on the specified type.

    Args:
        parameter (str): The parameter name.
        param_values (list): A list of parameter values.
        factor_type (str): Either 'management' or 'soils' to define the type of factor.
        manager_name (str, optional): The manager name (for a management factor).
        soil_node (str, optional): The soil node (for a soil factor).
        simulations (list, optional): The list of simulations.
        out_path_name (str, optional): The output path name (for a management factor).
        crop (str, optional): The crop name (for a soil factor).
        indices (list, optional): The indices (for soil factor).

    Returns:
        Factor: A Factor object containing the variable paths and the parameter values.
    """
    factor = factor_type.lower()
    if factor == 'management' and manager_name is not None:
        return Factor(
            variables=[dict(path=f'{simulations}.Manager.{manager_name}.{out_path_name}.{parameter}', param_values=i)
                       for i in param_values], variable_name=parameter)
    elif factor == 'soils' and soil_node is not None:
        return Factor(
            variables=[dict(path=f'{simulations}.Soil.{soil_node}.{crop}.{indices}.{parameter}', param_values=[i])
                       for i in param_values], variable_name=parameter)
    else:
        raise ValueError(f'Invalid factor type: {factor_type} and {manager_name or soil_node}')


# example
if __name__ == '__main__':
    from pprint import pprint

    bd = define_factor(parameter="BD", param_values=[1, 23, 157], factor_type='soils', soil_node='Physical')
    print(bd.variable_name)
    pprint(bd.variables, indent=4, width=1)

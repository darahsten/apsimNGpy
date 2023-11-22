"""
Interface to APSIM simulation models using Python.NET build on top of Matti Pastell farmingpy framework.
"""
import random, logging, pathlib
import pandas as pd
import sqlite3

from apsimNGpy.utililies.pythonet_config import LoadPythonnet

loader = LoadPythonnet()()
# C# imports
from System.Collections.Generic import *

import Models
from System import *

from collections import namedtuple
import json


def load_apsimx_from_string(path):
    Model_data = namedtuple('model_data', ['model', 'path', 'datastore', "DataStore"])
    try:
        with open(path, "r+") as apsimx:
            app_ap = json.load(apsimx)
        string_name = json.dumps(app_ap)
        fn = path
        Model = Models.Core.ApsimFile.FileFormat.ReadFromString[Models.Core.Simulations](string_name, None,
                                                                                         True, fileName=fn)
        if 'NewModel' in dir(Model):
            Model = Model.get_NewModel()

        datastore = Model.FindChild[Models.Storage.DataStore]().FileName
        DataStore = Model.FindChild[Models.Storage.DataStore]()
        named_tuple = Model_data(model=Model, path=path, datastore=datastore, DataStore=DataStore)
        return named_tuple
    except Exception as e:
        print(repr(e))
        raise


def _read_simulation(datastore, report_name=None):
    ''' returns all data frame the available report tables'''
    conn = sqlite3.connect(datastore)
    cursor = conn.cursor()

    # reading all table names

    table_names = [a for a in cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]

    table_list = []
    for i in table_names:
        table_list.append(i[0])
        # remove these
    rm = ['_InitialConditions', '_Messages', '_Checkpoints', '_Units']
    for i in rm:
        if i in table_list:
            table_list.remove(i)
            # start selecting tables
    select_template = 'SELECT * FROM {table_list}'

    # create data fram dictionary to keep all the tables
    dataframe_dict = {}

    for tname in table_list:
        query = select_template.format(table_list=tname)
        dataframe_dict[tname] = pd.read_sql(query, conn)
    # close the connection cursor
    conn.close()
    dfl = len(dataframe_dict)
    if len(dataframe_dict) == 0:
        print("the data dictionary is empty. no data has been returned")
        # else:
        # remove elements
        # print(f"{dfl} data frames has been returned")

    if report_name:
        return dataframe_dict[report_name]
    else:
        return dataframe_dict


def run(named_tuple_data, clean=False, multithread=True, read_db=False):
    """Run apsimx model in the simulations

        Parameters
        ----------
        simulations (__str_), optional
            List of simulation names to run, if `None` runs all simulations, by default `None`.
        clean (_-boolean_), optional
            If `True` remove existing database for the file before running, deafults to False`
        multithread, optional
            If `True` APSIM uses multiple threads, by default `True`
        """
    if multithread:
        runtype = Models.Core.Run.Runner.RunTypeEnum.MultiThreaded
    else:
        runtype = Models.Core.Run.Runner.RunTypeEnum.SingleThreaded

    # Clear old data before running
    results = None
    if clean:
        named_tuple_data.DataStore.Dispose()
        pathlib.Path(named_tuple_data.DataStore.FileName).unlink(missing_ok=True)
        named_tuple_data.DataStore.Open()

    runmodel = Models.Core.Run.Runner(named_tuple_data.model, True, False, False, None, runtype)
    e = runmodel.Run()
    if read_db:
        data = _read_simulation(named_tuple_data.datastore)
        return data


def run_model(path):
    """
    :param path: path to apsimx file
    :return: none
    """
    nt = load_apsimx_from_string(path)
    ap = run(nt)


def replace_soils():
    pass
if __name__ =="__main__":
    from apsimNGpy.base.base_data import LoadExampleFiles

    ex = load_apsimx_from_string(LoadExampleFiles().get_maize)


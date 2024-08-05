"""
This module offers a procedural alternative other than object-oriented approach provided in api and ApsimModel classes
"""
import os
from functools import singledispatch
from apsimNGpy.core.pythonet_config import LoadPythonnet
# now we can safely import C# libraries
from System.Collections.Generic import *
from System import *
from Models.Core.ApsimFile import FileFormat
from Models.Climate import Weather
from Models.Soils import Soil, Physical, SoilCrop, Organic
import Models
from Models.PMF import Cultivar
from apsimNGpy.core.apsim_file import XFile as load_model
import json
from os.path import (dirname, realpath, isfile)
from os import chdir
from pathlib import Path
import shutil
from collections import namedtuple


def load_from_dict(dict_data, out):
    str_ = json.dumps(dict_data)
    out = realpath(out)
    return Models.Core.ApsimFile.FileFormat.ReadFromString[Models.Core.Simulations](str_, None, True,
                                                                                    fileName=out)


def covert_to_IModel(object_to_convert):
    if isinstance(object_to_convert, Models.Core.ApsimFile.ConverterReturnType):
        return object_to_convert.get_NewModel()
    else:
        return object_to_convert


def load_model_from_dict(dict_model, out, met_file):
    """useful for spawning many simulation files"""
    met_file = realpath(met_file)
    in_model = dict_model or load_model
    memo = load_from_dict(dict_data=in_model, out=out)
    return memo


def load_from_path(path2file, method='string'):
    """"

    :param path2file: path to apsimx file
    :param method: str  with string, we direct the method to first convert the file
    into a string using json and then use the APSIM in-built method to load the file with file, we read directly from
    the file path. This is slower than the former.
    """
    f_name = realpath(path2file)
    with open(path2file, "r+", encoding='utf-8') as apsimx:
        app_ap = json.load(apsimx)
    string_name = json.dumps(app_ap)
    if method == 'string':
        __model = Models.Core.ApsimFile.FileFormat.ReadFromString[Models.Core.Simulations](string_name, None,
                                                                                           True,
                                                                                           fileName=f_name)
    else:
        __model = Models.Core.ApsimFile.FileFormat.ReadFromFile[Models.Core.Simulations](f_name, None, True)
    if isinstance(__model, Models.Core.ApsimFile.ConverterReturnType):

        return __model.get_NewModel()
    else:
        return __model


def load_apx_model(model=None, out=None, file_load_method='string', met_file=None, **kwargs):
    """
       >> we are loading apsimx model from file, dict, or in memory.
       >> if model is none, we will return a pre - reloaded one from memory.
       >> if out parameter is none, the new file will have a suffix _copy at the end
       >> if model is none, the name is ngpy_model
       returns a named tuple with an out path, datastore path, and IModel in memory
       """
    # name according to the order of preference
    if model is not None and isinstance(model, (str, Path)):
        out2 = f"{Path(model).parent}/{Path(model).stem}_copy.apsimx"
    else:
        out2 = None
    out1 = realpath(out) if out is not None else None
    out3 = realpath('ngpy_model.apsimx')
    _out = out1 or out2 or out3
    Model_data = namedtuple('model_data',
                            ['IModel', 'path', 'datastore', "DataStore", 'results', 'met_path'])

    @singledispatch
    def loader(_model):
        # this will raise not implemented error if the _model is not a dict, str, None, Models.Core.Simulation,
        # or a pathlib path object
        raise NotImplementedError(f"Unsupported type: {type(_model)}")

    @loader.register(dict)
    def _(_model: dict):
        # no need to copy the file
        assert _out, "out can not be none for dictionary data"
        return load_from_dict(_model, _out)

    @loader.register(str)
    def _(_model: str):
        # we first copy the file before loading it
        shutil.copy(_model, _out)

        return load_from_path(_out, file_load_method)

    @loader.register(Path)
    def _(_model: Path):
        # same as the string one, the difference is that this is a pathlib path object
        shutil.copy(_model, _out)
        return load_from_path(_out)

    @loader.register(type(None))
    def _(_model):
        # whenever the model is none, we return a preloaded dictionary in memory from the package
        return load_from_dict(load_model, out=_out)

    @loader.register(Models.Core.Simulations)
    def _(_model: Models.Core.Simulations):
        # it is already a model.core.Simulation object so we just return it
        return _model

    Model = loader(model)
    _Model = False
    if isinstance(Model, Models.Core.ApsimFile.ConverterReturnType):
        _Model = Model.get_NewModel()
    else:
        _Model = Model
    datastore = _Model.FindChild[Models.Storage.DataStore]().FileName
    DataStore = _Model.FindChild[Models.Storage.DataStore]()
    return Model_data(IModel=_Model, path=_out, datastore=datastore, DataStore=DataStore, results=None,
                      met_path=met_file)


def save_model_to_file(_model, out=None):
    """Save the model

        Parameters
        ----------
        out : str, optional path to save the model to
            reload: bool to load the file using the out path
            :param out: out path
            :param _model:APSIM Models.Core.Simulations object
            returns the filename or the specified out name
        """
    # Determine the output path
    _model = covert_to_IModel(object_to_convert=_model)
    final_out_path = out or '_saved_model.apsimx'

    # Serialize the model to JSON string
    json_string = Models.Core.ApsimFile.FileFormat.WriteToString(_model)

    # Save the JSON string to the determined output path
    with open(final_out_path, "w", encoding='utf-8') as f:
        f.write(json_string)
    return final_out_path


def recompile(_model, out=None, met_path=None):
    """ recompile without saving to disk useful for recombiling the same model on the go after updating management scripts

            Parameters
            ----------
            out : str, optional path to save the model to

                :param met_path: path to met file
                :param out: out path name for database reconfiguration
                :param _model:APSIM Models.Core.Simulations object
                returns named tuple with a recompiled model
            """
    # Determine the output path

    final_out_path = out or _model.FileName

    # Serialize the model to JSON string
    json_string = Models.Core.ApsimFile.FileFormat.WriteToString(_model)

    Model = Models.Core.ApsimFile.FileFormat.ReadFromString[Models.Core.Simulations](json_string, None, True,
                                                                                     fileName=final_out_path)
    _Model = False
    if isinstance(Model, Models.Core.ApsimFile.ConverterReturnType):
        _Model = Model.get_NewModel()
    else:
        _Model = Model
    datastore = _Model.FindChild[Models.Storage.DataStore]().FileName
    DataStore = _Model.FindChild[Models.Storage.DataStore]()
    # need to make ModelData a constant and named outside the script for consistency across scripts
    ModelData = namedtuple('model_data', ['IModel', 'path', 'datastore', "DataStore", 'results', 'met_path'])
    return ModelData(IModel=_Model, path=final_out_path, datastore=datastore, DataStore=DataStore,
                     results=None,
                     met_path=met_path)


if __name__ == '__main__':
    import time
    from pathlib import Path
    from apsimNGpy.core.base_data import load_default_simulations

    maze = load_default_simulations('maize', path=r'D:/p')
    soy = load_default_simulations('soybean', path=r'D:')

    # maze.initialise_model()
    chdir(Path.home())
    a = time.perf_counter()
    mod = load_from_path(maze.path, method='string')
    b = time.perf_counter()
    print(b - a, 'seconds', 'loading from file')
    aa = time.perf_counter()
    model = load_from_path(soy.path, method='string')
    print(time.perf_counter() - aa, 'seconds', 'loading from string')

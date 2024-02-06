import numpy as np
from typing import Tuple, Any
from apsimNGpy.core.apsim import ApsimModel
from apsimNGpy.core.base_data import LoadExampleFiles
from pathlib import Path
from apsimNGpy.weather import daymet_bylocation_nocsv, daymet_bylocation
from apsimNGpy.manager.soilmanager import DownloadsurgoSoiltables, OrganizeAPSIMsoil_profile
from apsimNGpy.utililies.spatial import create_fishnet1, create_apsimx_files

wd = Path.home()
from os.path import basename

maize = LoadExampleFiles(wd).get_maize

lon = -91.620369, 43.034534


def simulate_single_point(model: Any, location: Tuple[float, float], report, read_from_string=True, start=1990,
                          end=2020,
                          soil_series: str = 'domtcp', **kwargs):
    """
    Run a simulation of a given crop.
     model: Union[str, Simulations],
     location: longitude and latitude to run from, previously lonlat
     soil_series: str
     kwargs:
        copy: bool = False, out_path: str = None, read_from_string=True,

        soil_series: str = 'domtcp', thickness: int = 20, bottomdepth: int = 200,

        thickness_values: list = None, run_all_soils: bool = False

        report_name: str specifies the report or table name in the simulation, for which to read the reasults

        replace_weather: Set this boolean to true to download and replace the weather data based on the specified location.

        replace_soil: Set this boolean to true to download and replace the soil data using the given location details.

        mgt_practices: Provide a list of management decissions

    """
    thi = [150, 150, 200, 200, 200, 250, 300, 300, 400, 500]
    th = kwargs.get("thickness_values", thi)  # in case it is not supplied, we take thi
    simulator_model = ApsimModel(
        model, copy=kwargs.get('copy'), read_from_string=read_from_string, lonlat=location, thickness_values=th)

    sim_name = simulator_model.extract_simulation_name
    if kwargs.get('replace_weather', False):
        wname = model.strip('.apsimx') + '_w.met'
        wf = daymet_bylocation_nocsv(location, start, end, filename=wname)
        simulator_model.replace_met_file(wf, sim_name)

    if kwargs.get("replace_soil", False):
        table = DownloadsurgoSoiltables(location)
        sp = OrganizeAPSIMsoil_profile(table, thickness=20, thickness_values=th)
        sp = sp.cal_missingFromSurgo()
        simulator_model.replace_downloaded_soils(sp, sim_name)

    if kwargs.get("mgt_practices"):
        simulator_model.update_mgt(kwargs.get('mgt_practices'), sim_name)
    simulator_model.run(report_name=report)
    return simulator_model.results


def simulate_single_from_shape_file(wd, shape_file, model: Any, resolution, report, read_from_string=True, start=1990,
                                    end=2020,
                                    soil_series: str = 'domtcp', **kwargs):
    arr = create_fishnet1(shape_file, lon_step=resolution, lat_step=resolution, ncores=3)
    """
    Run a simulation of a given crop.
     model: Union[str, Simulations],
     location: longitude and latitude to run from, previously lonlat
     soil_series: str
     wd: pathlike stirng
     kwargs:
        copy: bool = False, out_path: str = None, read_from_string=True,

        soil_series: str = 'domtcp', thickness: int = 20, bottomdepth: int = 200,

        thickness_values: list = None, run_all_soils: bool = False

        report_name: str specifies the report or table name in the simulation, for which to read the reasults

        replace_weather: Set this boolean to true to download and replace the weather data based on the specified location.

        replace_soil: Set this boolean to true to download and replace the soil data using the given location details.

        mgt_practices: Provide a list of management decissions
        
        ncores: set the number of cores

    """
    arr = create_fishnet1(shape_file, lon_step=resolution, lat_step=resolution, ncores=kwargs.get('ncores', 4))
    thi = [150, 150, 200, 200, 200, 250, 300, 300, 400, 500]
    th = kwargs.get("thickness_values", thi)  # in case it is not supplied, we take thi
    simulator_model = ApsimModel(
        model, copy=kwargs.get('copy'), read_from_string=read_from_string, lonlat=None, thickness_values=th)

    sim_names = simulator_model.extract_simulation_name

    def worker(n):
        if kwargs.get('replace_weather', False):
            wname = model.strip('.apsimx') + '_w.met'
            wf = daymet_bylocation_nocsv(arr[n], start, end, filename=wname)
            simulator_model.replace_met_file(wf, sim_names)

        if kwargs.get("replace_soil", False):
            table = DownloadsurgoSoiltables(arr[n])
            sp = OrganizeAPSIMsoil_profile(table, thickness=20, thickness_values=th)
            sp = sp.cal_missingFromSurgo()
            simulator_model.replace_downloaded_soils(sp, sim_names)

        if kwargs.get("mgt_practices"):
            simulator_model.update_mgt(kwargs.get('mgt_practices'), sim_names)
        simulator_model.run(report_name=report)
        return simulator_model.results


md = {"Name": 'PostharvestillageMaize', 'Fraction': 0.001
      }
pp = simulate_single_point(maize, lon, replace_weather=True, replace_soil=True, mgt_practices=md, report='MaizeR')

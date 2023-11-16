"""
Interface to APSIM simulation models using Python.NET
author: Richard Magala
email: magalarich20@gmail.com
"""
import logging, pathlib
from typing import Union
import os
import numpy as np
import apsimNGpy.manager.weathermanager as weather
from apsimNGpy.manager.soilmanager import DownloadsurgoSoiltables, OrganizeAPSIMsoil_profile
from apsimNGpy.utililies.pythonet_config import get_apsimx_model_path, LoadPythonnet
py_config = LoadPythonnet()
py_config.start_pythonnet()
mod = py_config.load_apsim_model()
# now we can safely import any c# related libraries
from System.Collections.Generic import *
from Models.Core import Simulations
from System import *
from Models.Soils import Solute, Water, Chemical
from Models.Soils import Soil, Physical, SoilCrop, Organic
import Models
import time
from apsimNGpy.model.apsimpy import APSIMNG

# constants
REPORT_PATH = {'Carbon': '[Soil].Nutrient.TotalC/1000 as dyn', 'DUL': '[Soil].SoilWater.PAW as paw', 'N03':
    '[Soil].Nutrient.NO3.ppm as N03'}


# decorator to monitor performance
def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds to execute.")
        return result

    return wrapper


class SoilModel(APSIMNG):
    def __init__(self, model: Union[str, Simulations], copy:bool=False, out_path: str=None, read_from_string=True,
                 lonlat=None,
                 soil_series: str='domtcp', thickness: int=20, bottomdepth:int=200, thickness_values: list=None, run_all_soils: bool =False):
        super().__init__(model, read_from_string)
        """get suurgo soil tables and organise it to apsim soil profiles
        --------------------
        parameters
         soil_series (_str_) specifying the soils series default is domcp meaning dorminant soil series, users can specify any other series
         if known.
         lonlat {_lonlat_} of the specified location
         run_all_soils (__boolean_) if True, all soil series in the map units will be analysed if false the soil series will be the one determiend by paramter soil series
         model is a path of apsims file or apsimng object
         """
        self.lonlat = lonlat
        self.Nlayers = bottomdepth / thickness
        self.simulation_names = self.path
        self.soil_series = soil_series
        self.thickness = thickness
        self.out_path = out_path
        self.copy = copy
        self.run_all_soils = run_all_soils
        if not isinstance(thickness_values, np.ndarray):
            self.thickness_values = np.array(thickness_values, dtype=np.float64)  # apsim uses floating digit number
        else:
            self.thickness_values = thickness_values

    def _find_soil_solute(self, solute, simulation=None):
        sim = self._find_simulation(simulation)
        solutes = sim.FindAllDescendants[Models.Soils.Solute]()
        return [s for s in solutes if s.Name == solute][0]

    def _get_initial_chemical_values(self, name, simulation):
        s = self._find_soil_solute(name, simulation)
        return np.array(s.InitialValues)

    def _replace_initial_chemical_values(self, name, values, simulations):
        """_summary_

        Args:
            name (str): of the solutes e.g  NH4
            values (array): _values with equal lengths as the existing other variable
            simulations (str): simulation name in the root folder
        """
        simus = self.find_simulations(simulations)
        for sim in simus:
            solute = self._find_soil_solute(name, sim.Name)
            solute.InitialValues = values

    def show_cropsoil_names(self, simulations):
        for simu in self.find_simulations(simulations):
            pysoil = simu.FindDescendant[Physical]()
            soil_crop = pysoil.FindAllDescendants[SoilCrop]()
            # can be use to target specific crop
            for cropLL in soil_crop:
                print(cropLL.Name)

    def _replace_cropsoil_names(self, simulations, existing_crop_names, new_cropname):
        for simu in self.find_simulations(simulations):
            pysoil = simu.FindDescendant[Physical]()
            soil_crop = pysoil.FindAllDescendants[SoilCrop]()
            # can be use to target specific crop
            for crops in soil_crop:
                if crops.Name == existing_crop_names:
                    crops.Name = new_cropname

    def get_initial_no3(self, simulation=None):
        """Get soil initial NO3 content"""
        return self._get_initial_values("NO3", simulation)

    def _get_SSURGO_soil_profile(self, lonlat, run_all_soils=False):
        self.lonlat = None
        self.lonlat = lonlat
        self.dict_of_soils_tables = {}
        if not self.run_all_soils:
            self.soil_tables = DownloadsurgoSoiltables(self.lonlat, select_componentname=self.soil_series)
            for ss in self.soil_tables.componentname.unique():
                self.dict_of_soils_tables[ss] = self.soil_tables[self.soil_tables['componentname'] == ss]

        if self.run_all_soils:
            self.dict_of_soils_tables = {}
            self.soil_tables = DownloadsurgoSoiltables(self.lonlat)

            self.percent = self.soil_tables.prcent.unique()
            # create a dictionary of soil series
            self.unique_soil_series = self.soil_tables.componentname.unique()

            new_col = []
            for i in range(len(self.soil_tables['chkey'])):
                xi = str(list(self.soil_tables['chkey'])[i]) + "-" + list(self.soil_tables['componentname'])[i]
                new_col.append(xi)
            self.soil_tables["ch_comp"] = list(new_col)
            self.grouped = self.soil_tables.groupby('ch_comp')['prcent'].unique().apply(lambda x: x[0])
            self.component_percent_dict = self.grouped.to_dict()
            for ss in self.soil_tables.componentname.unique():
                self.dict_of_soils_tables[ss] = self.soil_tables[self.soil_tables['componentname'] == ss]
        return self

    @staticmethod
    def get_weather_online(lonlat, start, end):
        wp = weather.daymet_bylocation(lonlat, start=start, end=end)
        wpath = os.path.join(os.getcwd(), wp)
        return wpath

    @property
    def get_unique_soil_series(self):
        """this function collects the unique soil types

        Args:
            lonlat (_tuple_): longitude and latitude of the target location
        """
        try:
            soil_tables = DownloadsurgoSoiltables(self.lonlat)
            pr = soil_tables.prcent.unique()

            grouped = soil_tables.groupby('componentname')[
                'prcent'].unique()  # .agg(list) #.unique().apply(lambda x: x[0])
            component_percent_dict = grouped.to_dict()
            return component_percent_dict
        except Exception as e:
            raise

    def replace_soils(self, lonlat, simulation_names, verbose=False):
        self.thickness_replace = None
        if isinstance(self.thickness_values, np.ndarray):  # since it is alreaduy converted to an array
            self.thickness_replace = self.thickness_values

        else:
            tv = np.tile(float(self.thickness), int(self.Nlayers))
            self.thickness_replace = tv
        pss = self._get_SSURGO_soil_profile(lonlat)
        for keys in pss.dict_of_soils_tables.keys():
            self.soiltype = keys
            if verbose:
                print("Padding variabales for:", keys)
            self.soil_profile = OrganizeAPSIMsoil_profile(self.dict_of_soils_tables[keys],
                                                          thickness_values=self.thickness_values,
                                                          thickness=self.thickness)
            missing_properties = self.soil_profile.cal_missingFromSurgo()  # returns a list of physical, organic and cropdf each in a data frame
            physical_calculated = missing_properties[0]
            self.organic_calcualted = missing_properties[1]
            self.cropdf = missing_properties[2]
            # ps = self._get_SSURGO_soil_profile()

            # self.thickness_replace = list(np.full(shape=int(self.Nlayers,), fill_value=self.thickness*10,  dtype=np.float64))
            for simu in self.find_simulations(simulation_names):
                pysoil = simu.FindDescendant[Physical]()  # meaning physical soil node
                soil_crop = pysoil.FindChild[SoilCrop]()
                water = simu.FindDescendant[Water]()  # for the crop water parameters
                soil_crop.LL = physical_calculated.AirDry
                pysoil.DUL = physical_calculated.DUL
                pysoil.SAT = physical_calculated.SAT
                pysoil.BD = physical_calculated.BD
                pysoil.KS = physical_calculated.KS
                pysoil.LL15 = physical_calculated.LL15
                pysoil.ParticleSizeClay = physical_calculated.ParticleSizeClay
                pysoil.ParticleSizeSand = physical_calculated.ParticleSizeSand
                pysoil.ParticleSizeSilt = physical_calculated.ParticleSizeSilt
                water.InitialValues = physical_calculated.DUL

                water.Thickness = self.thickness_replace
                pysoil.AirDry = soil_crop.LL
                pysoil.Thickness = self.thickness_replace
                # replace the organic soils
            for simu in self.find_simulations(simulation_names):
                organic = simu.FindDescendant[Organic]()
                organic.Thickness = self.thickness_replace
                organic.SoilCNRatio = self.organic_calcualted.SoilCNRatio
                organic.FBiom = self.organic_calcualted.FBiom
                organic.FOM = self.organic_calcualted.FOM
                organic.FInert = self.organic_calcualted.FInert
                organic.Carbon = self.organic_calcualted.Carbon
                chemical = simu.FindDescendant[Chemical]()
                chemical.Thickness = self.thickness_replace
                # to do fix the crop management may use FindAllDescendants. it worked
                # XF = np.full(shape=self.Nlayers, fill_value=1,  dtype=np.float64)
                XF = np.tile(float(1), int(self.Nlayers))

            for simu in self.find_simulations(simulation_names):
                soil_crop = pysoil.FindAllDescendants[SoilCrop]()
                # can be use to target specific crop
                for cropLL in soil_crop:
                    cropLL.LL = pysoil.AirDry
                    cropLL.KL = self.organic_calcualted.cropKL
                    cropLL.XF = XF
                    cropLL.Thickness = self.thickness_replace
            if verbose:
                print("soil replacement complete")
            # self.run()
        return self
        # print(self.results)

    def replace_downloaded_soils(self, soil_tables, simulation_names):  # unique for my project
        self.thickness_replace = self.thickness_values
        physical_calculated = soil_tables[0]
        self.organic_calcualted = soil_tables[1]
        self.cropdf = soil_tables[2]
        for simu in self.find_simulations(simulation_names):
            pysoil = simu.FindDescendant[Physical]()  # meaning physical soil node
            soil_crop = pysoil.FindChild[SoilCrop]()
            water = simu.FindDescendant[Water]()  # for the crop water parameters
            soil_crop.LL = physical_calculated.AirDry
            pysoil.DUL = physical_calculated.DUL
            pysoil.SAT = physical_calculated.SAT
            pysoil.BD = physical_calculated.BD
            pysoil.KS = physical_calculated.KS
            pysoil.LL15 = physical_calculated.LL15
            pysoil.ParticleSizeClay = physical_calculated.ParticleSizeClay
            pysoil.ParticleSizeSand = physical_calculated.ParticleSizeSand
            pysoil.ParticleSizeSilt = physical_calculated.ParticleSizeSilt
            water.InitialValues = physical_calculated.DUL
            water.Thickness = None
            water.Thickness = self.thickness_replace
            pysoil.AirDry = soil_crop.LL
            pysoil.Thickness = self.thickness_replace
            # print(len(pysoil.Thickness))
            # replace the organic soils
        for simu in self.find_simulations(simulation_names):
            organic = simu.FindDescendant[Organic]()
            organic.Thickness = self.thickness_replace
            organic.SoilCNRatio = self.organic_calcualted.SoilCNRatio
            organic.FBiom = self.organic_calcualted.FBiom
            organic.FOM = self.organic_calcualted.FOM
            organic.FInert = self.organic_calcualted.FInert
            organic.Carbon = self.organic_calcualted.Carbon
            chemical = simu.FindDescendant[Chemical]()
            chemical.Thickness = self.thickness_values
            # to do fix the crop management may use FindAllDescendants. it worked
            # XF = np.full(shape=self.Nlayers, fill_value=1,  dtype=np.float64)
            XF = np.tile(float(1), int(self.Nlayers))

        for simu in self.find_simulations(simulation_names):
            soil_crop = pysoil.FindAllDescendants[SoilCrop]()
            # can be use to target specific crop
            for cropLL in soil_crop:
                cropLL.LL = pysoil.AirDry
                cropLL.KL = self.organic_calcualted.cropKL
                cropLL.XF = XF
                cropLL.Thickness = self.thickness_replace
        return self

    # print(self.results)
    def relace_initial_carbon(self, values, simulation_names):
        """Replaces initial carbon content of the organic module

        Args:
            values (_list_): liss of initial vlaues with length as the soil profile in the simulation file_
            simulation_names (_str_): Name of the simulation in the APSIM  file
        """
        for simu in self.find_simulations(simulation_names):
            organic = simu.FindDescendant[Organic]()
            organic.Carbon = np.array(values)

    def _change_met_file(self, lonlatmet=None, simulation_names=None):  # to be accessed only in this class
        """_similar to class weather management but just in case we want to change the weather within the subclass
        # uses exisitng start and end years to download the weather data
        """
        if lonlatmet == None:
            self.lonlat = self.lonlat
        else:
            self.lonlat = lonlatmet
        self.simulation_names = simulation_names

        start, end = self.extract_start_end_years()
        wp = weather.daymet_bylocation(self.lonlat, start, end)
        wpath = os.path.join(os.getcwd(), wp)
        wpath = os.path.join(os.getcwd(), wp)
        if self.simulation_names:
            sim_name = list(self.simulation_names)
        else:
            sim_name = self.extract_simulation_name  # because it is a property decorator
        self.replace_met_file(wpath, sim_name)
        return self

    def run_edited_file(self, simulations=None, clean=False, multithread=True):
        """Run simulations in this subclass if we want to clean the database, we need to
         spawn the path with one process to avoid os access permission eros


        Parameters
        ----------
        simulations, optional
            List of simulation names to run, if `None` runs all simulations, by default `None`.
        clean, optional
            If `True` remove existing database for the file before running, by default `True`
        multithread, optional
            If `True` APSIM uses multiple threads, by default `True`
        """
        runa, runb = Models.Core.Run.Runner.RunTypeEnum.MultiThreaded, Models.Core.Run.Runner.RunTypeEnum.SingleThreaded
        if multithread:
            runspeed = runa
        else:
            runspeed = runb
        self.results = None
        if clean:
            self._DataStore.Dispose()
            pathlib.Path(self._DataStore.FileName).unlink(missing_ok=True)
            self._DataStore.Open()
        if simulations is None:
            runmodel = Models.Core.Run.Runner(self.Model, True, False, False, None, runspeed)
            data_run = runmodel.Run()
        else:
            sims = self.find_simulations(simulations)
            # Runner needs C# list
            cs_sims = List[Models.Core.Simulation]()
            for s in sims:
                cs_sims.Add(s)
                runmodel = Models.Core.Run.Runner(cs_sims, True, False, False, None, runspeed)
                data_run = runmodel.Run()

        if (len(data_run) > 0):
            print(data_run[0].ToString())
        self.results = self._read_simulation()  # still wondering if this should be a static method
        return self.results

    def spin_up(self, report_name: str = 'Report', start=None, end=None, spin_var="Carbon"):
        """
        Perform a spin-up operation on the APSIM model.

        This method is used to simulate a spin-up operation in an APSIM model. During a spin-up, various soil properties or
        variables may be adjusted based on the simulation results.

        Parameters:
        ----------
        report_name : str, optional (default: 'Report')
            The name of the APSIM report to be used for simulation results.
        start : str, optional
            The start date for the simulation (e.g., '01-01-2023'). If provided, it will change the simulation start date.
        end : str, optional
            The end date for the simulation (e.g., '3-12-2023'). If provided, it will change the simulation end date.
        spin_var : str, optional (default: 'Carbon'). the difference between the start and end date will determine the spin-up period
            The variable representing the type of spin-up operation. Supported values are 'Carbon' or 'DUL'.

        Returns:
        -------
        self : ApsimModel
            The modified ApsimModel object after the spin-up operation.
        """
        insert_var = REPORT_PATH.get(spin_var)
        if start and end:
            self.change_simulation_dates(start, end)
        THICKNESS = self.extract_any_soil_physical("Thickness")
        th = np.array(THICKNESS)
        self.change_report(insert_var, report_name=report_name)
        rpn = insert_var.split(" ")[-1]
        self.run()
        DF = self.results[report_name]
        df_sel = DF.filter(regex=r'^{0}'.format(rpn), axis=1)
        if spin_var == 'Carbon':
            assert 'TotalC' in insert_var, "wrong report variable path: '{0}' supplied according to requested spin up " \
                                           "var".format(insert_var)
            bd = np.array(self.extract_any_soil_physical("BD"))
            cf = bd * th / 10  # this convert to percentage
            per = df_sel / cf
            new_carbon = [i for i in np.array(per).flatten()]
            self.replace_any_soil_organic(spin_var, new_carbon)
        if spin_var == 'DUL':
            assert 'PAW' in insert_var, "wrong report variable path: '{0}' supplied according to requested spin up var" \
                .format(insert_var)
            ll = np.array(self.extract_any_soil_physical("LL15"))
            dul = ll + df_sel
            dul = list(np.array(dul).flatten())
            self.replace_any_soil_physical(spin_var, dul)

            return self

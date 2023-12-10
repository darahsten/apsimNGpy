from typing import Tuple, Any
from .core.apsim import ApsimModel


def simulate(model: Any, location: Tuple[int, int], read_from_string=True, soil_series: str = 'domtcp', **kwargs):
    """
    Run a simulation of a given crop.
     model: Union[str, Simulations],
     location: longitude and latitude to run from, previously lonlat
     soil_series: str
     kwargs:
        copy: bool = False, out_path: str = None, read_from_string=True,
        soil_series: str = 'domtcp', thickness: int = 20, bottomdepth: int = 200,
        thickness_values: list = None, run_all_soils: bool = False

    """
    simulator_model = ApsimModel(
        model, copy=kwargs.get('copy'), read_from_string=read_from_string, lonlat=location, **kwargs)
    simulator_model.run()
    return simulator_model.results

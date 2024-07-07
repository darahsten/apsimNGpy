import os, sys; sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from apsimNGpy.core.pythonet_config import LoadPythonnet, APSIM_PATH
from apsimNGpy.core.apsim import ApsimModel
from apsimNGpy.core import weather
from apsimNGpy.core.base_data import DetectApsimExamples, LoadExampleFiles
__all__ = [LoadPythonnet, APSIM_PATH, ApsimModel, weather, DetectApsimExamples, LoadExampleFiles]

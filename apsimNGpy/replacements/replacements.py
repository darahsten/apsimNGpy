"""
This module attempts to provide abstract methods for paramters replacement to various nodes or childs in apsim simulations model
"""
from apsimNGpy.core.core import APSIMNG
from abc import ABC, abstractmethod
import copy
from apsimNGpy.utililies.utils import timer
import inspect


class ReplacementHolder(APSIMNG, ABC):
    def __init__(self, model, out_path=None, **kwargs):
        super().__init__(model, out_path=None, **kwargs)
        self._model = model
        self.out_path = out_path

    @abstractmethod
    def update_child_params(self, child: str, **kwargs):
        """Abstract method to replace parameters for a single child node"""
        pass

    @abstractmethod
    def update_children_params(self, children: tuple, **kwargs):
        """Abstract method to replace parameters for more than one child node"""
        pass


Nodes = [
    'cultivar',
    'manager',
    'weather',
    'soilphysical',
    'soilorganic',
    'soilchemical',
    'soilwater',
    'soilorganicMatter',
    'clock'
]


class Replacements(ReplacementHolder):

    def __init__(self, model, out_path=None, **kwargs):
        super().__init__(model, out_path, **kwargs)
        # Map action types to method names
        # this will hold lower key
        self._methods = {
            'cultivar': 'edit_cultivar',
            'manager': 'update_mgt',
            'weather': 'replace_met_file',
            'soilphysical': 'replace_any_soil_physical',
            'soilorganic': 'replace_any_soil_organic',
            'soilchemical': 'replace_any_solute',
            'soilwater': 'replace_crop_soil_water',
            'soilorganicmatter': 'change_som',
            'clock': 'change_simulation_dates'
        }
        self.methods = None
        self.out_path = out_path

        # define them with human-readable formats

    def replacement_methods(self, child):

        if child in self._methods:
            return getattr(self, self._methods[child])
        else:
            raise TypeError(f"Unknown node: {child}, children should be any of {self._methods.keys()}")

    def update_mgt_by_path(self, *, path, param_values, fmt='.'):
        parameters_guide = ['simulations_name', 'Manager', 'manager_name', 'out_path_name', 'parameter_name']
        parameters = ['simulations', 'Manager', 'Name', 'out']
        args = path.split(fmt)
        if len(args) != len(parameters_guide):
            join_p = ".".join(parameters_guide)
            raise ValueError(f"Invalid path '{path}' expected path should follow {join_p}")
        args = [(p := f"'{arg}'") if " " in arg and fmt != " " and '[' not in arg else arg for arg in args]
        _eval_params = [self._try_literal_eval(arg) for arg in args]

        _eval_params[1] = {'Name': _eval_params[2], _eval_params[-1]: param_values},
        parameters[1] = 'management'

        _param_values = dict(zip(parameters, _eval_params))

        return self.update_mgt(**_param_values)

    def update_child_params(self, child: str, **kwargs):
        """Abstract method to perform various parameters replacements in apSim model. :param child: (str): name of
        e.g., weather space is allowed for more descriptive one such a soil organic not case-sensitive :keyword
        kwargs: these correspond to each node you are editing. Please see the corresponding methods for each node
        the followingare the parameters for each node:
        'cultivar': ('simulations','CultivarName', 'commands', 'values'),
        'manager': ('management', 'simulations', 'out'),
        'weather': ('weather_file', 'simulations'),
        'soilphysical': ('parameter', 'param_values', 'simulation'),
        'soilorganic': ('parameter', 'param_values', 'simulation'),
        'soilchemical': ('parameter', 'param_values', 'simulation'),
        'soilwater': ('parameter', 'param_values', 'simulation'),
        'soilorganicmatter': ('simulations', 'inrm', 'icnr'),
        'clock': ('start_date', 'end_date', 'simulations')
        """
        # Convert keys to lowercase
        _child = child.lower().replace(" ", "")
        # no need to filter paramters as it takes one child node
        return self.replacement_methods(_child)(**kwargs)

    def update_children_params(self, children: tuple, **kwargs):
        """Method to perform various parameters replacements in apSim model.
        :param children: (str): name of e.g., weather space is allowed for more descriptive one such a soil organic not case-sensitive
        :keyword kwargs: these correspond to each node you are editing see the corresponding methods for each node
        the following are available for each child passed to children
        'cultivar': ('simulations','CultivarName', 'commands', 'values'),
        'manager': ('management', 'simulations', 'out'),
        'weather': ('weather_file', 'simulations'),
        'soilphysical': ('parameter', 'param_values', 'simulation'),
        'soilorganic': ('parameter', 'param_values', 'simulation'),
        'soilchemical': ('parameter', 'param_values', 'simulation'),
        'soilwater': ('parameter', 'param_values', 'simulation'),
        'soilorganicmatter': ('simulations', 'inrm', 'icnr'),
        'clock': ('start_date', 'end_date', 'simulations')
        """
        # chd = iter(children)
        # while True:
        #     child = next(chd, None)
        #     if child is None:
        #         break
        for child in children:
            child = child.lower().replace(" ", "")
            method = self.replacement_methods(child)
            sig = inspect.signature(method)
            args = {k: v for k, v in kwargs.items() if k in sig.parameters.keys()}
            method(**args)
        return self


if __name__ == '__main__':
    from pathlib import Path

    import os

    os.chdir(Path.home())
    from apsimNGpy.core.base_data import load_default_simulations, weather_path

    mn = load_default_simulations(crop='Maize')
    ce = Replacements(mn.path, out_path='a.apsimx')
    mets = list(Path(weather_path).glob('*.met'))
    met = os.path.realpath(list(mets)[0])
    met2 = os.path.realpath(list(mets)[5])

    # the method make_replacements can be chained with several other action types
    mgt = {'Name': 'Sow using a variable rule', 'Population': 8.5},
    model = ce.update_child_params(child='weather', weather_file=met)
    mgt = {'Name': 'Sow using a variable rule', 'Population': 7.5},
    chilredren = 'Manager', 'weather', 'SoilOrganicMatter'
    ce.update_children_params(children=chilredren, icnr=148, weather_file=met2, management=mgt)
    xi = ce.extract_user_input('Sow using a variable rule')

    ce.show_met_file_in_simulation()
    from apsimNGpy.core.base_data import load_default_simulations

    soybean_model = load_default_simulations(crop='soybean')
    soybean_model.run("Report")
    res = soybean_model.results
    print(res)

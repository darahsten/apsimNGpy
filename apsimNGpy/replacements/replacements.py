from apsimNGpy.core.core import APSIMNG
from abc import ABC, abstractmethod


class Editor(APSIMNG):
    def __init__(self, model, **kwargs):
        super().__init__(model, **kwargs)
        self._model = model

    def change_cultivar(self, **kwargs):
        self.edit_cultivar(**kwargs)
        return self

    def edit_mgt_practices(self, **kwargs):
        self.update_mgt(**kwargs)
        return self

    def edit_weather(self, **kwargs):
        self.replace_met_file(**kwargs)

    def update_soil_physical(self, **kwargs):
        self.replace_any_soil_physical(**kwargs)
        return self

    def update_soil_organic(self, **kwargs):
        self.replace_any_soil_organic(**kwargs)

    def update_soil_chemical(self, **kwargs):
        self.replace_any_solute(**kwargs)
        return self

    def update_soil_water(self, **kwargs):
        self.replace_crop_soil_water(**kwargs)


class ReplacementHolder(APSIMNG, ABC):
    def __init__(self, model, **kwargs):
        super().__init__(model, **kwargs)
        self._model = model

    @abstractmethod
    def make_replacements(self, action_type, **kwargs):
        """Abstract method to perform various actions. Must be implemented by subclasses."""
        pass


class Replacements(ReplacementHolder):
    def __init__(self, model, **kwargs):
        super().__init__(model, **kwargs)
        # Map action types to method names
        self.action_map = {
            'cultivar': self.edit_cultivar,
            'manager': self.update_mgt,
            'weather': self.replace_met_file,
            'soil_physical': self.replace_any_soil_physical,
            'soil_organic': self.replace_any_soil_organic,
            'soil_chemical': self.replace_any_solute,
            'soil_water': self.replace_crop_soil_water,
            'soil_organic_matter': self.change_som
        }

    def make_replacements(self, node, **kwargs):
        """Perform various actions based on the action_type."""
        if node not in self.action_map:
            raise ValueError(f"Unknown action_type: {node}, node should be any of {self.action_map.keys()}")
        return self.action_map[node](**kwargs)


if __name__ == '__main__':
    from pathlib import Path

    import os
    os.chdir(Path.home())
    from apsimNGpy.core.base_data import load_default_simulations, weather_path
    mn = load_default_simulations('Maize')
    ce= Replacements(mn.path)
    mets = Path(weather_path).glob('*.met')
    met = os.path.realpath(list(mets)[0])
    # the method make_replacements can be chained with several other action types
    model = ce.make_replacements(node='weather',weather_file = met).make_replacements(node='weather',weather_file = met)



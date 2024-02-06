from apsimNGpy.manager.soilmanager import DownloadsurgoSoiltables, OrganizeAPSIMsoil_profile
from apsimNGpy.weather import daymet_bylocation_nocsv
from apsimNGpy.core.apsim import ApsimModel


def download_soil_table(x):
    try:
        cod = x
        table = DownloadsurgoSoiltables(cod)
        th = [150, 150, 200, 200, 200, 250, 300, 300, 400, 500]
        sp = OrganizeAPSIMsoil_profile(table, thickness=20, thickness_values=th).cal_missingFromSurgo()
        return {x: sp}
    except Exception as e:
        print("Exception Type:", type(e), "has occured")
        print(repr(e))


def simulator_worker(dictio):
    def worker(row):
        kwargs = dictio
        report = kwargs.get('report_name')
        ID = row['ID']
        model = row['file_name']
        simulator_model = ApsimModel(
            model, copy=kwargs.get('copy'), read_from_string=read_from_string, lonlat=None, thickness_values=th)
        sim_names = simulator_model.extract_simulation_name
        location = row['location']

        if kwargs.get('replace_weather', False):
            wname = model.strip('.apsimx') + '_w.met'
            wf = daymet_bylocation_nocsv(location, start, end, filename=wname)
            simulator_model.replace_met_file(wf, sim_names)

        if kwargs.get("replace_soil", False):
            table = DownloadsurgoSoiltables(location)
            sp = OrganizeAPSIMsoil_profile(table, thickness=20, thickness_values=th)
            sp = sp.cal_missingFromSurgo()
            simulator_model.replace_downloaded_soils(sp, sim_names)

        if kwargs.get("mgt_practices"):
            simulator_model.update_mgt(kwargs.get('mgt_practices'), sim_names)
        simulator_model.run(report_name=report)
        return simulator_model.results


lon = -92.70166631, 42.26139442
lm = download_soil_table(lon)
print(lm)

from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from time import perf_counter
from tqdm import tqdm
from multiprocessing import cpu_count
from apsimNGpy.utils.run import run_model, read_simulation
from core.soils import download_surgo_soil_tables, APSIMSoilProfile


def run_in_parallel(func, apsim_files, num_cores=None, use_threads=False, bar_format="", **kwargs):
    """
    Args:
    - apsim_files (list): A list of APSIMX  files to be run in parallel.
    - num_cores (int, optional): The number of CPU cores or threads to use for parallel processing. If not provided,
    it defaults to 50% of available CPU cores.
    - use_threads (bool, optional): If set to True, the function uses thread pool execution; otherwise, it uses process
    pool execution. Default is False.
    bar_format: A string format use to update the progress bar with the progress.

    Returns:
    - returns a generator object containing the path to the datastore or sql databases

    """
    files = set(apsim_files)
    num_cores_2_use = num_cores if num_cores else int(cpu_count() * 0.5)
    a = perf_counter()
    bar_format = bar_format or "Running apsimx files: {percentage:3.0f}% completed"
    extra_args = kwargs.get('args')

    PoolExecutor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
    with PoolExecutor(num_cores_2_use) as pool:
        if not extra_args:
            futures = [pool.submit(func, file) for file in files]
        else:
            futures = [pool.submit(func, file, *extra_args) for file in files]
        progress = tqdm(total=len(futures), position=0, leave=True,
                        bar_format=bar_format)
        for future in as_completed(futures):
            yield future.result()
            progress.update(1)
        progress.close()
    print(perf_counter() - a, 'seconds', f'to run {len(files)} files')


def run_apsimx_files_in_parallel(apsim_files, num_cores=None, use_threads=False):
    """
    Run APSIMX simulation from multiple files in parallel.

    Args:
    - apsim_files (list): A list of APSIMX  files to be run in parallel.
    - num_cores (int, optional): The number of CPU cores or threads to use for parallel processing. If not provided, it defaults to 50% of available CPU cores.
    - use_threads (bool, optional): If set to True, the function uses thread pool execution; otherwise, it uses process pool execution. Default is False.

    Returns:
    - returns a generator object containing the path to the datastore or sql databases

    Example:
    ```python
    # Example usage of read_result_in_parallel function

    from apsimNgpy.parallel.process import run_apsimxfiles_in_parallel
    simulation_files = ["file1.apsimx", "file2.apsimx", ...]  # Replace with actual database file names

    # Using processes for parallel execution
    result_generator = run_apsimx_files_in_parallel(simulation_files, num_cores=4, use_threads=False)
    ```

    Notes:
    - This function efficiently reads db file results in parallel.
    - The choice of thread or process execution can be specified with the `use_threads` parameter.
    - By default, the function uses 50% of available CPU cores or threads if `ncores` is not provided.
    - Progress information is displayed during execution.
    - Handle any exceptions that may occur during execution for robust processing.
    """
    # remove duplicates. because duplicates will be susceptible to race conditioning in paralell computing
    run_in_parallel(run_model, apsim_files, num_cores=num_cores, use_threads=use_threads)


def read_result_in_parallel(apsim_result_files, num_cores=None, use_threads=False, report_name="Report"):
    """

    Read APSIMX simulation databases results from multiple files in parallel.

    Args:
    - iterable_files (list): A list of APSIMX db  files to be read in parallel.
    - ncores (int, optional): The number of CPU cores or threads to use for parallel processing. If not provided, it defaults to 50% of available CPU cores.
    - use_threads (bool, optional): If set to True, the function uses thread pool execution; otherwise, it uses process pool execution. Default is False.
    -  report_name the name of the  report table defaults to "Report" you can use None to return all

    Returns:
    - generator: A generator yielding the simulation data read from each file.

    Example:
    ```python
    # Example usage of read_result_in_parallel function
    from  apsimNgpy.parallel.process import read_result_in_parallel

    simulation_files = ["file1.db", "file2.db", ...]  # Replace with actual database file names

    # Using processes for parallel execution
    result_generator = read_result_in_parallel(simulation_files, ncores=4, use_threads=False)

    # Iterate through the generator to process results
    for data in result_generator:
        print(data)
    it depends on the type of data but pd.concat could be a good option on the returned generator
    ```

    Notes:
    - This function efficiently reads db file results in parallel.
    - The choice of thread or process execution can be specified with the `use_threads` parameter.
    - By default, the function uses 50% of available CPU cores or threads if `ncores` is not provided.
    - Progress information is displayed during execution.
    - Handle any exceptions that may occur during execution for robust processing.
    """

    # remove duplicates. because duplicates will be susceptible to race conditioning in paralell computing
    files = set(apsim_result_files)
    bar_format = "Reading file databases: {percentage:3.0f}% completed"
    extra_args = {"args": ["Report"]}
    run_in_parallel(
        read_simulation, files, num_cores=num_cores, use_threads=use_threads,
        bar_format=bar_format, **extra_args)


def download_soil_tables(iterable, use_threads=False, ncores=None, soil_series=None):
    """

    Downloads soil data from SSURGO (Soil Survey Geographic Database) based on lonlat coordinates.

    Args:
    - iterable (iterable): An iterable containing lonlat coordinates as tuples or lists.
    - use_threads (bool, optional): If True, use thread pool execution. If False, use process pool execution. Default is False.
    - ncores (int, optional): The number of CPU cores or threads to use for parallel processing. If not provided, it defaults to 40% of available CPU cores.
    - soil_series (None, optional): [Insert description if applicable.]

    Returns:
    - a generator: with dictionaries containing calculated soil profiles with the corresponding index positions based on lonlat coordinates.

    Example:
    ```python
    # Example usage of download_soil_tables function
    from your_module import download_soil_tables

    lonlat_coords = [(x1, y1), (x2, y2), ...]  # Replace with actual lonlat coordinates

    # Using threads for parallel processing
    soil_profiles = download_soil_tables(lonlat_coords, use_threads=True, ncores=4)

    # Iterate through the results
    for index, profile in soil_profiles.items():
        process_soil_profile(index, profile)
    ```

    Notes:
    - This function efficiently downloads soil data and returns calculated profiles.
    - The choice of thread or process execution can be specified with the `use_threads` parameter.
    - By default, the function utilizes available CPU cores or threads (40% of total) if `ncores` is not provided.
    - Progress information is displayed during execution.
    - Handle any exceptions that may occur during execution to avoid aborting the whole download

    """

    def _concat(x):
        try:
            cod = iterable[x]
            table = download_surgo_soil_tables(cod)
            th = [150, 150, 200, 200, 200, 250, 300, 300, 400, 500]
            sp = APSIMSoilProfile(table, thickness=20, thickness_values=th).cal_missingFromSurgo()
            return {x: sp}
        except Exception as e:
            print("Exception Type:", type(e), "has occured")
            print(repr(e))

    if not ncores:
        ncores_2use = int(cpu_count() * 0.4)
        print(f"using: {ncores_2use} cpu cores")
    else:
        ncores_2use = ncores
    if not use_threads:
        with ThreadPoolExecutor(max_workers=ncores_2use) as tpool:
            futures = [tpool.submit(_concat, n) for n in range(len(iterable))]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='downloading soil_tables...: {percentage:3.0f}% completed')
            for future in as_completed(futures):
                progress.update(1)
                yield future.result()
            progress.close()
    else:
        with ProcessPoolExecutor(max_workers=ncores_2use) as ppool:
            futures = [ppool.submit(_concat, n) for n in range(len(iterable))]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='downloading soil_tables..: {percentage:3.0f}% completed')
            for future in as_completed(futures):
                progress.update(1)
                yield future.result()
            progress.close()


if __name__ == '__main__':
    def fn(x, p):
        return 1 * x + p

    lm = run_in_parallel(fn, range(100000), True, 6, 5)
    pm = list(lm)

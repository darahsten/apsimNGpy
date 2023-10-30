from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import glob, os, sys
from time import perf_counter
from tqdm import tqdm
from os.path import dirname
from os.path import join as opj
from apsimNGpy.utililies.run_utils import run_model, read_simulation


# _______________________________________________________________
def run_apsimxfiles_in_parallel(iterable_files, ncores, use_threads=False):
    """
    files: lists of apsimx simulation files
    ncores  =no of cores or threads to use (integer)
    use_thread: if true thread pool executor will be used if false processpool excutor will be called (boolean)
    """
    # remove duplicates. because duplicates will be susceptible to race conditioning in paralell computing
    files = set(iterable_files)
    if not use_threads:
        a = perf_counter()
        with ProcessPoolExecutor(ncores) as pool:
            futures = [pool.submit(run_model, i) for i in files]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='Running apsimx files: {percentage:3.0f}% completed')
            for future in as_completed(futures):
                future.result()  # retrieve the result (or use it if needed)
                progress.update(1)
            progress.close()
        print(perf_counter() - a, 'seconds', f'to run {len(files)} files')
    else:
        a = perf_counter()
        with ThreadPoolExecutor(ncores) as tpool:
            futures = [tpool.submit(run_model, i) for i in files]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='Running apsimx files: {percentage:3.0f}% completed')
            # Iterate over the futures as they complete
            for future in as_completed(futures):
                future.result()  # retrieve the result (or use it if needed)
                progress.update(1)
            progress.close()
        print(perf_counter() - a, 'seconds', f'to run {len(files)} files')


def read_result_in_parallel(iterable_files, ncores, use_threads=False):
    """
    files: lists of apsimx simulation files
    ncores  =no of cores or threads to use (integer)
    use_thread: if true thread pool executor will be used if false processpool excutor will be called (boolean)
    """
    # remove duplicates. because duplicates will be susceptible to race conditioning in paralell computing
    files = set(iterable_files)
    if not use_threads:
        a = perf_counter()
        with ProcessPoolExecutor(ncores) as pool:
            futures = [pool.submit(read_simulation, i) for i in files]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='reading file databases: {percentage:3.0f}% completed')
            # Iterate over the futures as they complete
            for future in as_completed(futures):
                data = future.result()
                yield data  # retrieve and store it in a generator
                progress.update(1)
            progress.close()
        print(perf_counter() - a, 'seconds', f'to read {len(files)} apsimx files databases')
    else:
        a = perf_counter()
        with ThreadPoolExecutor(ncores) as tpool:
            futures = [tpool.submit(read_simulation, i) for i in files]
            progress = tqdm(total=len(futures), position=0, leave=True,
                            bar_format='reading file databases: {percentage:3.0f}% completed')
            # Iterate over the futures as they complete
            for future in as_completed(futures):
                yield future.result()  # retrieve and store it in a generator
                progress.update(1)
            progress.close()
        print(perf_counter() - a, 'seconds', f'to read {len(files)} apsimx database files')


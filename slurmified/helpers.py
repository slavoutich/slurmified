from .core import Cluster
from distributed import Client, progress


def map_interactive(n_workers, **kwargs):
    """Generates a map function `map_func(func, arg_list)`, that executes
    function `func` over argument list `arg_list`, creating temporary
    Dask.distributed cluster.

    Parameters
    ----------
    n_workers : int
        Number of workers to request. Calculation will be run as soon as some
        of them are allocated.
    **kwargs : arguments, accepted by `slurmified.Cluster` constructor.
    """

    def map_func(func, arg_list):
        with Cluster(**kwargs) as cluster:
            cluster.start_workers(n_workers)
            with Client(cluster) as client:
                _results = client.map(func, arg_list)
                progress(_results, notebook=False)
                result_list = client.gather(_results)
                print("")  # If there will be more output after a progressbar

        return result_list

    return map_func

import logging
import os
import shutil
import sys
import socket
import slurmpy
import subprocess

from distributed import LocalCluster
from distributed.utils import sync, ignoring
from distributed.core import CommClosedError
from time import time, sleep
from toolz import merge
from tornado.gen import TimeoutError

logger = logging.getLogger(__name__)


class Cluster:

    def _wait_workers_start(self, n=1, timeout=None):
        """ Wait for number of workers, seen by scheduler, will become not less
        than n and return True. If timeout is reached, and it is still
        not happens, return False """
        dt = timeout or self._wait_timeout
        end_time = time() + dt
        while True:
            if self.n_workers >= n:
                return True
            if time() > end_time:
                return False
            sleep(self._wait_timestep)

    def _start_local_cluster(self, **kwargs):
        ip = kwargs.pop("ip", socket.gethostbyname(self._hostname))
        scheduler_port = kwargs.pop("scheduler_port", 0)
        self._local_cluster = LocalCluster(
            n_workers=0, ip=ip, scheduler_port=scheduler_port, **kwargs)
        logger.info("Started local scheduler at {addr}".
                    format(addr=self.scheduler_address))

    def __init__(self, slurm_kwargs=None, hostname=None, task_name=None,
                 nanny=True, bokeh=True, bokeh_port=None, timeout=10.,
                 extra_path=None, tmp_dir=None, **kwargs):
        """
        Dask.Distribued workers launched via SLURM workload manager

        Parameters
        ----------
        slurm_kwargs : dict
            A dictionary with arguments, passed to SLURM batch script
            (see Examples). If None, defaults to empty dictionary.
        hostname : None or string
            Hostname of a controller node, visible by other SLURM nodes.
            If None, determined automatically through 'socket.gethostname()'.
        task_name : string or None
            Name of the job, passed to SLURM. If None, defaults to
            'dask-workers'.
        nanny : boolean
            Start Dask workers in nanny process for management.
            Default is True.
        bokeh : boolean
            Whether to launch Bokeh Web UI
            Default is True.
        bokeh_port: None or int
            Bokeh port for dask-worker. None means default.
        timeout : float
            Default time to wait until workers start
            (see ``self.start_workers``).
        extra_path : None or str or List of str
            Extra module path values, that are injected to the workers via
            PYTHONPATH environment variable
        tmp_dir : str or None
            Directory for temporary files. If not specified, defaults to
            "slurmified_files" in working directory.
            For now it is assumed, that it is accessible from all nodes of a
            cluster. If you need more clever behaviour, please file a bug.

        **kwargs: dict
            Keyword arguments, passed directly to 'distributed.LocalCluster'
            constructor.

        Examples
        --------
        >>> from slurmified import Cluster
        >>> slurm_kwargs = {
        ...     'partition': 'default',
        ...     'mem-per-cpu': '100',
        ...     'time': '1-00:00:00'
        ... }
        >>> cluster = Cluster(slurm_kwargs)
        >>> cluster.start_workers(10)
        >>> from distributed import Client
        >>> client = Client(cluster)
        >>> future = client.submit(lambda x: x + 1, 10)
        >>> future.result()
        11
        """
        self._hostname = hostname or socket.gethostname()
        self._start_local_cluster(**kwargs)

        self._slurm_kwargs = slurm_kwargs.copy() if slurm_kwargs else {}
        nthreads1 = self._slurm_kwargs.pop("cpus-per-task", None)
        nthreads2 = self._slurm_kwargs.pop("c", None)
        self._nthreads = nthreads1 or nthreads2 or 1
        self._jobid = None

        self._task_name = task_name or "dask-workers"
        self._wait_timeout = timeout
        self._wait_timestep = 1

        self._worker_exec = os.path.join(sys.exec_prefix, 'bin', 'dask-worker')
        logger.info("Using dask-worker executable '{exe}'".
                    format(exe=self._worker_exec))
        self._nanny = nanny
        self._bokeh = bokeh
        self._bokeh_port = bokeh_port
        if isinstance(extra_path, str):
            self._extra_path = [extra_path]
        else:
            self._extra_path = extra_path

        self._tmp_dir = tmp_dir or os.path.abspath("slurmified_files")
        if not os.path.exists(self._tmp_dir):
            os.makedirs(self._tmp_dir)
            self._remove_tmp_dir = True
        else:
            self._remove_tmp_dir = False

    @property
    def scheduler(self):
        return self._local_cluster.scheduler

    @property
    def scheduler_address(self):
        return ('{hostname}:{port}'.
                format(hostname=self._hostname, port=self.scheduler.port))

    @property
    def n_workers(self):
        return len(self.scheduler.workers)

    def start_workers(self, n=1, n_min=None, timeout=None, **kwargs):
        """Start Dask workers via SLURM batch script. If workers are started
        already, they are terminated. Returns self.

        Parameters
        ----------
        n: int
            Number of workers to start.
        n_min: None or int
            Minimal number of workers launched, needed to start calculations.
            Function waits, until it is reached and exits. If it is not
            achieved until ``timeout``, RuntimeError will be emited. If None,
            wunction will wait for all ``n`` workers to start, but error would
            never be emited, only warning.
        timeout: None or int
            Time in seconds to wait for workers to start. If it is reached, and
            workers are not started, warning is emited. If None, default is
            used (provided in constructor).
        **kwargs: dict
            Dictionary with strings as keys and values, can be used to override
            SLURM kwargs, passed to the constructor.
        """
        if self._jobid:
            self.stop_workers()
        slurm_kwargs = merge(
            self._slurm_kwargs, kwargs or {},
            {"array": "0-{}".format(n-1), "cpus-per-task": self._nthreads}
        )
        if self._extra_path:
            pythonpath_cmd = (
                "[[ -z \"$PYTHONPATH\" ]] && "
                "export PYTHONPATH=\"{new_entries}\" || "
                "export PYTHONPATH=\"{new_entries}:$PYTHONPATH\""
                .format(new_entries=":".join(self._extra_path))
            )
        else:
            pythonpath_cmd = ""

        s = slurmpy.Slurm(self._task_name, slurm_kwargs=slurm_kwargs,
                          scripts_dir=self._tmp_dir)
        self._jobid = s.run(
            pythonpath_cmd + "\n" +
            " ".join((self._worker_exec,
                      "--nthreads", str(self._nthreads),
                      "--nprocs", "1",
                      "--reconnect",
                      "--nanny" if self._nanny else "--no-nanny",
                      "--bokeh" if self._bokeh else "--no-bokeh",
                      ("--bokeh-port {}".format(self._bokeh_port) if
                       self._bokeh_port else ""),
                      "--local-directory \"{}\"".format(self._tmp_dir),
                      self.scheduler_address))
        )
        if self._wait_workers_start(n_min or n, timeout):
            m = ("Started {n} workers, job number {jobid}"
                 .format(n=self.n_workers, jobid=self._jobid))
            logger.info(m)
        elif n_min:
            m = ("Not enough workers to continue "
                 "({n}, minimal provided {n_min})"
                 .format(n=self.n_workers, n_min=n_min))
            self.stop_workers()
            raise RuntimeError(m)
        else:
            m = ("Timeout is reached while waiting for {n} workers to start. "
                 "{n_started} actually started. Job number {jobid}."
                 .format(n=n, n_started=self.n_workers, jobid=self._jobid))
            logger.warning(m)
        return self

    def stop_workers(self):
        """ Stop running workers. """
        try:
            with ignoring(TimeoutError, CommClosedError, OSError):
                sync(loop=self._local_cluster.loop,
                     func=self.scheduler.retire_workers,
                     remove=True)
        finally:
            if self._jobid:
                try:
                    subprocess.check_call(("scancel", str(self._jobid)))
                except subprocess.CalledProcessError as ex:
                    m = ("scancel returned non-zero exit status {code} while "
                         "stopping Slurm job number {jobid} for workers. "
                         "You should check manually whether they are "
                         "terminated successfully."
                         .format(code=ex.returncode, jobid=self._jobid))
                    logger.error(m)
                finally:
                    self._jobid = None

    def _start(self):
        return self._local_cluster._start()

    def close(self):
        """ Close the cluster. """
        logger.info("Closing workers and cluster")
        if self._jobid:
            self.stop_workers()
        self._local_cluster.close()

        if self._remove_tmp_dir:
            shutil.rmtree(self._tmp_dir)
            self._remove_tmp_dir = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

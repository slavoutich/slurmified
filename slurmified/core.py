import logging
import os
import sys
import socket
import slurmpy
import subprocess

from distributed import LocalCluster
from distributed.utils import sync
from time import time, sleep
from toolz import merge

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
                 timeout=10., **kwargs):
        """
        Dask.Distribued workers launched via SLURM workload manager

        Parameters
        ----------
        slurm_kwargs: dict
            A dictionary with arguments, passed to SLURM batch script
            (see Examples). If None, defaults to empty dictionary.
        hostname: None or string
            Hostname of a controller node, visible by other SLURM nodes.
            If None, determined automatically through 'socket.gethostname()'.
        task_name: string or None
            Name of the job, passed to SLURM. If None, defaults to
            'dask-workers'.
        timeout: float
            Default time to wait until workers start
            (see ``self.start_workers``).

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

        self.workdir = os.getcwd()
        self._worker_exec = os.path.join(sys.exec_prefix, 'bin', 'dask-worker')
        logger.info("Using dask-worker executable '{exe}'".
                    format(exe=self._worker_exec))

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
        s = slurmpy.Slurm(self._task_name, slurm_kwargs)
        self._jobid = s.run(
            "cd {}\n".format(self.workdir) +
            " ".join((self._worker_exec,
                      "--nthreads", str(self._nthreads),
                      "--nprocs", "1",
                      "--reconnect",
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
            sync(loop=self._local_cluster.loop,
                 func=self.scheduler.retire_workers,
                 workers=self.scheduler.workers_to_close(),
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

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass

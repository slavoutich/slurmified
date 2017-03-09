Dask on SLURM
=============

Deploy a Dask.distributed_ cluster on top of a cluster running a
SLURM_ workload manager.

Written under influence and with code borrowing from Dask-DRMAA_ project.

.. _Dask.distributed: http://distributed.readthedocs.io/en/latest/
.. _SLURM: https://slurm.schedmd.com/
.. _Dask-DRMAA: https://github.com/dask/dask-drmaa/

Example
-------

Launch cluster from Python code and do some simple calculation:

.. code-block:: python

   from slurmified import Cluster
   slurm_kwargs = {
       'mem-per-cpu': '100',
       'time': '1-00:00:00'
   }
   cluster = Cluster(slurm_kwargs)
   cluster.start_workers(10)
   from distributed import Client
   client = Client(cluster)
   future = client.submit(lambda x: x + 1, 10)
   future.result()  # returns 11

If you want cluster to terminate automatically after calculation finished,
you can use the following:

.. code-block:: python

   from slurmified import Cluster
   from distributed import Client
   slurm_kwargs = {
       'mem-per-cpu': '100',
       'time': '1-00:00:00'
   }
   inputs = list(range(0, 100))
   with Client(Cluster(slurm_kwargs).start_workers(10)) as client:
       incremented = client.map(lambda x: x+1, inputs)
       inverted = client.map(lambda x: -x, incremented)
       outputs = client.gather(inverted)
   print(outputs)  # prints [-1, .. , -100]

Installation
------------

Via pip::

    pip install git+https://gitlab.kwant-project.org/slavoutich/slurmified.git --upgrade

or::

    pip install git+https://github.com/slavoutich/slurmified.git --upgrade

or::

    pip install git+https://gitlab.com/slavoutich/slurmified.git --upgrade

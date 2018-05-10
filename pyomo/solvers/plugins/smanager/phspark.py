import pkg_resources

from pyomo.opt import AsynchronousSolverManager, pyomo
from pyspark import SparkConf, SparkContext

__all__ = ["SolverManager_PHSpark"]


class SolverManager_PHSpark(AsynchronousSolverManager):
    pyomo.util.plugin.alias('phspark',
                            doc='Test')

    def __init__(self, host, port, verbose=False):
        # Spark connection endpoint
        # Not using default parameters because of how options are handled on PHAlgorithmBuilder
        if host is not None:
            self.host = host
        else:
            self.host = "localhost"

        if port is not None:
            self.port = port
        else:
            self.port = 7077

        self._verbose = verbose
        self._ah = {}
        self._bulk_transmit_mode = False
        self._bulk_task_dict = {}
        self._task_name_to_worker = {}

        # RDD list of solver servers
        self.server_pool = []
        self._rddWorkerList = None

        AsynchronousSolverManager.__init__(self)

    def clear(self):
        AsynchronousSolverManager.clear(self)

        self._verbose = False
        self._ah = {}

        self.server_pool = []
        # self._worker_list.wait_all()
        # self._worker_list.destroy()

    def begin_bulk(self):
        self._bulk_transmit_mode = True

    def end_bulk(self):
        """Probably not going to use this"""
        self._bulk_transmit_mode = False
        # if len(self._bulk_task_dict):
        #    self._worker_list.map(lambda worker: worker.process(nextTask))

    # TODO: check when worker count and task count don't match

    def _extract_result(self):
        """
        Using self._results_waiting.
        Maybe sparks lets checking task status
        """

    def _perform_queue(self, ah, *args, **kwds):
        """
        Perform the queue operation.  This method returns the
        ActionHandle, and the ActionHandle status indicates whether
        the queue was successful.
        """

        # the PH solver server expects no non-keyword arguments.
        if len(args) > 0:
            raise RuntimeError("ERROR: The _perform_queue method of PH "
                               "spark solver manager received position input "
                               "arguments, but accepts none.")

        if "action" not in kwds:
            raise RuntimeError("ERROR: No 'action' keyword supplied to "
                               "_perform_queue method of PH spark solver manager")

        if "queue_name" not in kwds:
            raise RuntimeError("ERROR: No 'queue_name' keyword supplied to "
                               "_perform_queue method of PH spark solver manager")
        # TODO: maybe do this
        # if "broadcast" not in kwds:
        #     raise RuntimeError("ERROR: No 'broadcast' keyword supplied to "
        #                        "_perform_queue method of PH spark solver manager")

        queue_name = kwds["queue_name"]
        # broadcast = kwds["broadcast"]

        if "verbose" not in kwds:
            # we always want to pass a verbose flag to the solver server.
            kwds["verbose"] = False

        if "generateResponse" in kwds:
            generateResponse = kwds.pop("generateResponse")
        else:
            generateResponse = True

        # TODO: this collect might kill performance
        # if broadcast:
        self._rddWorkerList.foreach(lambda worker: worker.process(kwds))
        self._rddWorkerList.unpersist()
        # else:
        #     if len(self._bulk_task_dict) != len(self.server_pool):
        #         raise AttributeError("TODO")
        #     else:
        #         self.server_pool.foreach(lambda worker: worker.process(self._bulk_task_dict.pop()))

        # only populate the action_handle-to-task dictionary is a
        # response is expected.
        # TODO: this doesn't work but it should
        # if generateResponse:
        #     self._ah[ah['id']] = ah

        return ah

    def _perform_wait_any(self):
        """
        Perform the wait_any operation.  This method returns an
        ActionHandle with the results of waiting.  If None is returned
        then the ActionManager assumes that it can call this method
        again.  Note that an ActionHandle can be returned with a dummy
        value, to indicate an error.
        """

        # TODO: this enters a loop for now
        print("Not implemented [phspark::_perform_wait_any]")
        return self._ah

    def acquire_servers(self, servers_requested, timeout=None):

        # TODO: Manage errors
        spark_url = "spark://" + self.host + ":" + str(self.port)

        if self._verbose:
            print("Initializing spark context on %s" % spark_url)

        # conf = SparkConf().setMaster("spark://" + self.host + ":" + str(self.port)).setAppName("pyomo")

        # TODO: connect to actual spark
        conf = SparkConf().setMaster("local").setAppName("Test")

        sc = SparkContext(conf=conf)
        dependency_path = pkg_resources.resource_filename('pyomo.pysp', 'phsolverserver.py')
        print ("Trying to add " + dependency_path)
        sc.addPyFile(dependency_path)

        from phsolverserver import PHSparkWorker
        for i in range(servers_requested):
            self.server_pool.append(PHSparkWorker())

        self._rddWorkerList = sc.parallelize(self.server_pool).persist()

        print("Requested %d servers" % servers_requested)
        print("Not implemented [phspark::acquire_servers]")

    def release_servers(self, shutdown=False):
        print("Not implemented [phspark::release_servers]")




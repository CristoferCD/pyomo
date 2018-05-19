import pkg_resources
from pyspark.serializers import CloudPickleSerializer

from pyomo.opt.parallel.manager import ActionStatus

from pyomo.opt import AsynchronousSolverManager, pyomo
from pyspark import SparkConf, SparkContext, StorageLevel

import pyutilib.pyro

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
        self._bulk_transmit_mode = False
        self._bulk_task_dict = {}
        self._task_name_to_worker = {}
        # map from task id to the corresponding action handle.
        # we only retain entries for tasks for which we expect
        # a result/response.
        self._ah = {}
        # the list of cached results obtained from the dispatch server.
        # to avoid communication overhead, grab any/all results available,
        # and then cache them here - but return one-at-a-time via
        # the standard _perform_wait_any interface. the elements in this
        # list are simply tasks - at this point, we don't care about the
        # queue name associated with the task.
        self._results_waiting = []

        # RDD list of solver server names
        self.server_pool = []
        self._rddWorkerList = None
        self._sparkContext = None

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

        def _do_parallel_bulk(worker, task_dict):
            if worker.id in task_dict.value:
                for task in task_dict.value[worker.id]:
                    worker.process(task)
                task_dict.value[worker.id] = []
            return worker

        self._bulk_transmit_mode = False
        task_dict = self._sparkContext.broadcast(self._bulk_task_dict)
        if len(self._bulk_task_dict):
            self._rddWorkerList = self._rddWorkerList.map(lambda worker: _do_parallel_bulk(worker, task_dict))
        self._bulk_task_dict = {}

    def _perform_queue(self, ah, *args, **kwds):
        """
        Perform the queue operation.  This method returns the
        ActionHandle, and the ActionHandle status indicates whether
        the queue was successful.
        """

        def _do_parallel_work(worker, task, id):
            if worker.id == id:
                worker.process(task)
            return worker

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

        task = pyutilib.pyro.Task(data=kwds,
                                  id=ah.id,
                                  generateResponse=generateResponse)

        print("")
        print ("[PHSpark_Manager]: Requested action " + task['data']['action'])
        print ("[PHSpark_Manager]: Task id " + str(task['id']))
        print("Requested action on queue with name: " + str(queue_name))

        if self._bulk_transmit_mode:
            if queue_name not in self._bulk_task_dict:
                self._bulk_task_dict[queue_name] = []
            self._bulk_task_dict[queue_name].append(task)

        else:
            self._rddWorkerList = self._rddWorkerList.map(lambda worker:
                                                          _do_parallel_work(worker, task, queue_name))

        # only populate the action_handle-to-task dictionary is a
        # response is expected.
        if generateResponse:
            self._ah[task['id']] = ah

        return ah

    def _perform_wait_any(self):
        """
        Perform the wait_any operation.  This method returns an
        ActionHandle with the results of waiting.  If None is returned
        then the ActionManager assumes that it can call this method
        again.  Note that an ActionHandle can be returned with a dummy
        value, to indicate an error.
        """

        def _pop_result(worker):
            worker.get_results()
            return worker

        def _get_result_pair(worker):
            results = worker.get_results()
            return worker, results

        if len(self._results_waiting) > 0:
            return self._extract_result()

        # Test 1 Wrong results (probably persistence error)
        self._rddWorkerList = self._rddWorkerList.map(lambda worker: _get_result_pair(worker))
        result_list = self._rddWorkerList.map(lambda pair: pair[1]).collect()
        self._rddWorkerList = self._rddWorkerList.map(lambda pair: pair[0])

        # Test 2 (Dictionary error, fail to find item 'ConstraintTotalAcreage'
        # self._rddWorkerList = self._rddWorkerList.map(lambda worker: _get_result_pair(worker))
        #
        # processed_pairs = self._rddWorkerList.collect()
        #
        # worker_list = []
        # result_list = []
        # for worker, result in processed_pairs:
        #     worker_list.append(worker)
        #     result_list.append(result)
        #
        # self._rddWorkerList = self._sparkContext.parallelize(worker_list)

        # Test 3 Wrong results (probably persistence error)
        # result_list = self._rddWorkerList.map(lambda worker: worker.get_results()).collect()
        # self._rddWorkerList = self._rddWorkerList.map(lambda worker: _pop_result(worker))

        all_results = [item for sublist in result_list for item in sublist]
        self._rddWorkerList.persist()

        if len(all_results) > 0:
            for task in all_results:
                self._results_waiting.append(task)

    def acquire_servers(self, servers_requested, timeout=None):

        # TODO: Manage errors
        spark_url = "spark://" + self.host + ":" + str(self.port)

        if self._verbose:
            print("Initializing spark context on %s" % spark_url)

        # conf = SparkConf().setMaster("spark://" + self.host + ":" + str(self.port)).setAppName("pyomo")

        # TODO: connect to actual spark
        conf = SparkConf().setMaster("local").setAppName("Pyomo")
        #conf = SparkConf().setMaster("spark://localhost:7077").setAppName("Pyomo")

        self._sparkContext = SparkContext(conf=conf, serializer=CloudPickleSerializer())
        dependency_path = pkg_resources.resource_filename('pyomo.pysp', 'phsolverserver.py')
        print ("Trying to add " + dependency_path)
        self._sparkContext.addPyFile(dependency_path)

        from phsolverserver import PHSparkWorker
        server_list = []
        for i in range(servers_requested):
            server_list.append(PHSparkWorker(i))
            self.server_pool.append(i)

        self._rddWorkerList = self._sparkContext.parallelize(server_list).persist()

        print("Requested %d servers" % servers_requested)
        print("Not implemented [phspark::acquire_servers]")

    def release_servers(self, shutdown=False):
        print("Not implemented [phspark::release_servers]")

    #
    # a utility to extract a single result from the _results_waiting
    # list.
    #

    def _extract_result(self):

        if len(self._results_waiting) == 0:
            raise RuntimeError("There are no results available for "
                               "extraction from the PHPyro solver manager "
                               "- call to _extract_result is not valid.")

        task = self._results_waiting.pop(0)

        if task['id'] in self._ah:
            print("[PHSpark_Manager] Extracting result for task with id: " + str(task['id']))
            ah = self._ah[task['id']]
            self._ah[task['id']] = None
            ah.status = ActionStatus.done
            # TBD - what is the 'results' object - can we just load
            # results directly into there?
            self.results[ah.id] = task['result']
            return ah
        else:
            # if we are here, this is really bad news!
            raise RuntimeError("The PHPyro solver manager found "
                               "results for task with id="+str(task['id'])+
                               " - but no corresponding action handle "
                               "could be located!")



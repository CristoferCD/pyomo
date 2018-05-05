from pyomo.opt import AsynchronousSolverManager, pyomo

__all__ = ["SolverManager_PHSpark"]


class SolverManager_PHSpark(AsynchronousSolverManager):
    pyomo.util.plugin.alias('phspark',
                            doc='Test')

    def __init__(self, host="localhost", port=7070, verbose=False):
        # Spark connection endpoint
        self.host = host
        self.port = port

        self._verbose = verbose
        self._ah = {}
        self._bulk_transmit_mode = False
        self._bulk_task_dict = {}

        # RDD list of solver servers
        self._worker_list = {}

        AsynchronousSolverManager.__init__(self)

    def clear(self):
        AsynchronousSolverManager.clear(self)

        self._verbose = False
        self._ah = {}

        self._worker_list = []
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

        if "broadcast" not in kwds:
            raise RuntimeError("ERROR: No 'broadcast' keyword supplied to "
                               "_perform_queue method of PH spark solver manager")

        queue_name = kwds["queue_name"]
        broadcast = kwds["broadcast"]

        if "verbose" not in kwds:
            # we always want to pass a verbose flag to the solver server.
            kwds["verbose"] = False

        if "generateResponse" in kwds:
            generateResponse = kwds.pop("generateResponse")
        else:
            generateResponse = True

        if broadcast:
            self._worker_list.foreach(lambda worker: worker.process(kwds))
        else:
            if len(self._bulk_task_dict) != len(self._worker_list):
                raise AttributeError("TODO")
            else:
                self._worker_list.foreach(lambda worker: worker.process(self._bulk_task_dict.pop()))

        # only populate the action_handle-to-task dictionary is a
        # response is expected.
        if generateResponse:
            self._ah[ah['id']] = ah

        return ah






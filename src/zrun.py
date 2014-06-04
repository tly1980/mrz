#!/usr/bin/env python

import importlib
import multiprocessing
import logging
import argparse

import zmq
import msgpack

import utils

logging.basicConfig(level=logging.INFO)

class Worker(object):

    def __init__(self, 
            _id,
            task_addr, 
            result_addr, 
            task_exec):
        self._id = _id
        self.task_addr = task_addr
        self.result_addr = result_addr
        self.task_exec = task_exec
        the_name = task_exec.__class__.__name__ if hasattr(task_exec, '__class__') else str(task_exec)
        self.logger = logging.getLogger(
            "{}.{}".format(
            _id, the_name)
        )
        self.task_exec.logger = logging.getLogger(
            "{}.exec.{}".format(
            _id, the_name)
        )

    def init_zmq(self):
        self.logger.info(
            "init_zmq - worker, task_addr: %s, result_addr: %s",
            self.task_addr,
            self.result_addr)

        context = zmq.Context()
        self.task_socket = context.socket(
            zmq.PULL )

        self.result_socket = context.socket(
            zmq.PUSH )

        self.task_socket.connect(self.task_addr)
        self.result_socket.connect(self.result_addr)

    def run(self):
        while True:
            raw_task = self.task_socket.recv()
            task = msgpack.unpackb(raw_task)
            result = self.task_exec.do(
                *task['args'], 
                **task['kwargs'])
            if hasattr(result, '__iter__'):
                for r in result:
                    self.logger.info("shotting %s with %s", self.result_addr, r )
                    self.result_socket.send(
                        msgpack.packb(r)
                    )
            else:
                self.result_socket.send(
                    msgpack.packb(result)
                )


class Producer(object):

    def __init__(self, 
            _id,
            result_addr, 
            _exec):
        self._id = _id
        self.result_addr = result_addr
        self._exec = _exec
        the_name = _exec.__class__.__name__ if hasattr(_exec, '__class__') else str(_exec)
        self.logger = logging.getLogger(
            "{}.{}".format(
            self._id, the_name)
        )
        self.task_exec.logger = logging.getLogger(
            "{}.exec.{}".format(self._id, the_name)
        )

    def init_zmq(self):
        self.logger.info(
            "init_zmq: %s result_addr: %s", self.result_addr)

        context = zmq.Context()

        self.result_socket = context.socket(
            zmq.PUSH )

        self.result_socket.connect(self.result_addr)

    def run(self):
        for t in self._task.tasks():
            self.result_socket.send(msgpack.packb(t))


class DummyTask(object):
    def do(self, *args, **kwargs):
        result = "Dummy recv: args: %s, kwargs: %s" %(
            str(args), str(kwargs))
        self.logger.info(result)
        return result


def load_exec(name):
    idx = name.rfind('.')
    if idx != -1:
        mpath, klass = name[:idx], name[idx+1:]
        m = importlib.import_module(mpath)
        return getattr(m, klass)
    else:
        return globals()[name]

def start_worker(
    name, 
    task_addr, 
    result_addr='ipc://null.sock', 
    number=1, prefix='w', exec_kwargs={}):
    exec_klass = load_exec(name)

    def fun(the_id):
        task_exec = exec_klass(**exec_kwargs)
        w = Worker(
                '{}.{}'.format(prefix, the_id),
                task_addr, result_addr, task_exec)
        w.init_zmq()
        w.run()

    for i in xrange(number-1):
        p = multiprocessing.Process(target=fun, 
            args=(i+1, ))
        p.daemon = True
        p.start()

    # now start the default process
    fun(0)

def start_producer(
    name, result_addr='ipc://null.sock', 
    number=1, prefix='p', exec_kwargs={}):
    exec_klass = load_exec(name)

    def fun(the_id):
        task_exec = exec_klass(**exec_kwargs)
        w = Worker(
                '{}.{}'.format(prefix, the_id), result_addr, task_exec)
        w.init_zmq()
        w.run()

    for i in xrange(number-1):
        p = multiprocessing.Process(target=fun, 
            args=(i+1, ))
        p.daemon = True
        p.start()

    # now start the default process
    fun(0)


def main(args):
    params = utils.yaml_xtract(args.params)
    klass_params = utils.yaml_xtract(args.klass_params)

    params['klass_params'] = klass_params

    if args.fun == 'start_worker':
        start_worker(**params)
    elif args.fun == 'start_producer':
        start_producer(**params)

AP = argparse.ArgumentParser()

AP.add_argument(
    'fun', help='the function you want to run')
AP.add_argument(
    'params', 
    default=None,
    help="parameter for the function in yaml format. It could be file if you starts with ./ or / ",
    type=str)

AP.add_argument(
    'klass_params', 
    default=None,
    help="Second set ofr parameters.",
    type=str)


if __name__ == '__main__':
    main(AP.parse_args())


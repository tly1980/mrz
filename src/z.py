#!/usr/bin/env python
import argparse
import multiprocessing

import yaml
import zmq
import msgpack

from utils import shout, yaml_xtract

def shoot(addr, *args, **kwargs):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(addr)
    to_send = {'args': args, 'kwargs': kwargs}
    print "Sending to {}: {} ".format(addr, to_send)
    data = msgpack.packb(to_send)
    socket.send(data)

def stream(pull_addr, push_addr):
    print "Stream: {} -> {}".format(pull_addr, push_addr)
    context = zmq.Context()
    fe = context.socket(zmq.PULL)
    fe.bind(pull_addr)

    be = context.socket(zmq.PUSH)
    be.bind(push_addr)

    zmq.device(zmq.STREAMER, fe, be)


def null(addr='ipc://null.sock', sock_type='PULL', verbose=False):
    context = zmq.Context()
    socket = context.socket(getattr(zmq, sock_type))
    socket.bind(addr)
    while True:
        raw_data = socket.recv()
        if not verbose:
            shout('.')
        else:
            print msgpack.unpackb(raw_data)

ACTIONS = {
    'shoot': shoot,
    'stream': stream,
    'null': null
}

AP = argparse.ArgumentParser()

AP.add_argument(
    'fun', type=str,
    help="action to call. The available actions are: {}".format(ACTIONS.keys())
    )

AP.add_argument(
    '-a',
    '--args', type=str,
    help="parameter for the function in yaml format. It could be file if you starts with ./ or / ")

AP.add_argument(
    '-kw', 
    '--kwargs', 
    default=None,
    help="parameter for the function in yaml format. It could be file if you starts with ./ or / ",
    type=str)


def main(args):

    the_args = yaml_xtract(args.args) if args.args else {}
    kwargs = yaml_xtract(args.kwargs) if args.kwargs else {}
    fun = ACTIONS[args.fun]
    fun(*the_args, **kwargs)

if __name__ == '__main__':
    main(AP.parse_args())

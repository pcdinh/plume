#!/usr/bin/env python

import time
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from plume.gen_py.flume import ThriftFlumeEventServer
from plume.gen_py.flume.ttypes import ThriftFlumeEvent, RawEvent

class FlumeEventServer(object):
    PRIORITY_FATAL   = 0
    PRIORITY_ERROR   = 1
    PRIORITY_WARNING = 2
    PRIORITY_INFO    = 3
    PRIORITY_DEBUG   = 4
    PRIORITY_TRACE   = 5

    def __init__(self, host="localhost", port=35873):
        self.socket = TSocket.TSocket(host, port)
        # transport = TTransport.TFramedTransport(socket)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = FlumeMasterAdminServer.Client(self.protocol)
        self.transport.open()

    def append(self, priority, body, host, timestamp, nanos, fields):
        event = ThriftFlumeEvent(
            timestamp = timestamp,
            priority = priority,
            body = body,
            host = host,
            nanos = nanos,
            fields = fields
        )
        self.client.append(event)

    def raw_append(self, event):
        rawevent = RawEvent(event)
        self.client.rawAppend(event)

    def acked_append(self, priority, body, host, timestamp, nanos, fields):
        event = ThriftFlumeEvent(
            timestamp = timestamp,
            priority = priority,
            body = body,
            host = host,
            nanos = nanos,
            fields = fields
        )
        status = self.client.ackedAppend(event)
        return status

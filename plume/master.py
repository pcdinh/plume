#!/usr/bin/env python

import time
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from plume.gen_py.mastercontrol import FlumeMasterAdminServer
from plume.gen_py.mastercontrol.ttypes import FlumeMasterCommandThrift

class FlumeMaster(object):
    STATE_HELLO = 0
    STATE_IDLE = 1
    STATE_CONFIGURING = 2
    STATE_ACTIVE = 3
    STATE_ERROR = 4
    STATE_LOST = 5
    STATE_DECOMMISSIONED = 6

    def __init__(self, host="localhost", port=35873):
        self.socket = TSocket.TSocket(host, port)
        # transport = TTransport.TFramedTransport(socket)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = FlumeMasterAdminServer.Client(self.protocol)
        self.transport.open()

    def submit(self, command, *args):
        cmd = FlumeMasterCommandThrift(command, args)
        return self.client.submit(cmd)

    def is_success(self, cmdid):
        return self.client.isSuccess(cmdid)

    def is_failure(self, cmdid):
        return self.client.isFailure(cmdid)

    def execute(self, command, *args):
        cmdid = self.submit(command, *args)
        for i in range(20):
            if self.is_success(cmdid):
                return
            if self.is_failure(cmdid):
                raise Exception("Command '%s%r' failed" % (command, args if args else []))
            time.sleep(0.01 * (2**i))
        raise Exception("Command '%s%r' timed out" % (command, args if args else []))

    def get_node_statuses(self):
        statuses = self.client.getNodeStatuses()
        return dict((k, v.__dict__) for k, v in statuses.items())

    def get_configs(self):
        configs = self.client.getConfigs()
        return dict((k, v.__dict__) for k, v in configs.items())

    def has_cmdid(self, cmdid):
        return self.client.hasCmdId(cmdid)

    def get_mappings(self, physicalnode):
        return self.client.getMappings(physicalnode)

    def config(self, node, source, sink):
        self.execute("config", node, source, sink)

    def decommission(self, logicalnode):
        self.execute("decommission", logicalnode)

    def multiconfig(self, spec):
        self.execute("multiconfig", spec)

    def noop(self, delaymillis=None):
        self.execute("noop", *([str(delaymillis)] if delaymillis else []))

    def refresh(self, spec):
        self.execute("refresh", spec)

    def refresh_all(self):
        self.execute("refreshAll")

    def spawn(self, physicalnode, logicalnode):
        self.execute("spawn", physicalnode, logicalnode)

    def unmap(self, physicalnode, logicalnode):
        self.execute("unmap", physicalnode, logicalnode)

    def unmap_all(self):
        self.execute("unmapAll")

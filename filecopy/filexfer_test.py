# Copyright 2011 Obsidian Research Corp. GLPv2, see COPYING.
import pickle
import socket
import contextlib
import os, sys
import time
from mmap import mmap
from collections import namedtuple
import rdma.ibverbs as ibv;
from rdma.tools import clock_monotonic
import rdma.path
import rdma.vtools

ip_port = 4444
tx_depth = 100
memsize = 16*1024*1024

infotype = namedtuple('infotype', 'path addr rkey size iters')

class Endpoint(object):
    ctx = None;
    pd = None;
    cq = None;
    mr = None;
    peerinfo = None;

    def __init__(self,fp,sz,dev):
        self.ctx = rdma.get_verbs(dev)
        self.cc = self.ctx.comp_channel();
        self.cq = self.ctx.cq(2*tx_depth,self.cc)
        self.poller = rdma.vtools.CQPoller(self.cq);
        self.pd = self.ctx.pd()
        self.qp = self.pd.qp(ibv.IBV_QPT_RC,tx_depth,self.cq,tx_depth,self.cq);
        self.mem = mmap(fp, sz)
        self.mr = self.pd.mr(self.mem,
                             ibv.IBV_ACCESS_LOCAL_WRITE|ibv.IBV_ACCESS_REMOTE_WRITE)
        self.size = sz

    def __enter__(self):
        return self;

    def __exit__(self,*exc_info):
        return self.close();

    def close(self):
        print "Endpoint:close"
        if self.ctx is not None:
            self.ctx.close();

    def connect(self, peerinfo):
        self.peerinfo = peerinfo
        self.qp.establish(self.path,ibv.IBV_ACCESS_REMOTE_WRITE);

    def rdma(self):
        swr = ibv.send_wr(wr_id=0,
                          remote_addr=self.peerinfo.addr,
                          rkey=self.peerinfo.rkey,
                          sg_list=self.mr.sge(),
                          opcode=ibv.IBV_WR_RDMA_WRITE,
                          send_flags=ibv.IBV_SEND_SIGNALED)

        n = 1
        depth = min(tx_depth, n, self.qp.max_send_wr)

        tpost = clock_monotonic()
        for i in xrange(depth):
            self.qp.post_send(swr)

        completions = 0
        posts = depth
        for wc in self.poller.iterwc(timeout=3):
            if wc.status != ibv.IBV_WC_SUCCESS:
                raise ibv.WCError(wc,self.cq,obj=self.qp);
            completions += 1
            if posts < n:
                self.qp.post_send(swr)
                posts += 1
                self.poller.wakeat = rdma.tools.clock_monotonic() + 1;
            if completions == n:
                break;
        else:
            raise rdma.RDMAError("CQ timed out");

        tcomp = clock_monotonic()

        rate = self.size/1e6/(tcomp-tpost)
        print "%.1f MB/sec" % rate

def client_mode(hostname,infilename,dev):
    f = open(infilename, "r+")
    sz = os.path.getsize(infilename)
    with Endpoint(f.fileno(), sz, dev) as end:
        ret = socket.getaddrinfo(hostname,str(ip_port),0,
                                 socket.SOCK_STREAM);
        ret = ret[0];
        with contextlib.closing(socket.socket(ret[0],ret[1])) as sock:
            sock.connect(ret[4]);

            path = rdma.path.IBPath(dev,SGID=end.ctx.end_port.default_gid);
            rdma.path.fill_path(end.qp,path,max_rd_atomic=0);
            path.reverse(for_reply=False);

            sock.send(pickle.dumps(infotype(path=path,
                                            addr=end.mr.addr,
                                            rkey=end.mr.rkey,
                                            size=end.mem.size(),
                                            iters=1)))
            buf = sock.recv(1024)
            peerinfo = pickle.loads(buf)

            end.path = peerinfo.path;
            end.path.reverse(for_reply=False);
            end.path.end_port = end.ctx.end_port;

            print "path to peer %r\nMR peer raddr=%x peer rkey=%x"%(
                end.path,peerinfo.addr,peerinfo.rkey);

            end.connect(peerinfo)
            # Synchronize the transition to RTS
            sock.send("Ready");
            sock.recv(1024);
            startTime = time.time();
            end.rdma()
            endTime = time.time();
            print "-- rmda end: elapsed time = %f " % (endTime - startTime)

            sock.shutdown(socket.SHUT_WR);
            sock.recv(1024);

            print "---client end"
        print "---sock close"
    print "--- endpoint close"

def server_mode(outfilename, dev):
    ret = socket.getaddrinfo(None,str(ip_port),0,
                             socket.SOCK_STREAM,0,
                             socket.AI_PASSIVE);
    ret = ret[0];
    with contextlib.closing(socket.socket(ret[0],ret[1])) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(ret[4]);
        sock.listen(1)

        print "Listening port..."
        s,addr = sock.accept()
        with contextlib.closing(s):
            totalStartTime = time.time();

            peerInfoStartTime = time.time();
            buf = s.recv(1024)
            peerinfo = pickle.loads(buf)
            peerInfoEndTime = time.time();
            print "sz = ", peerinfo.size
            print "--peerinfo: elapsed = %f secs" % (peerInfoEndTime - peerInfoStartTime)

            fseekStartTime = time.time();
            f = open(outfilename, "w+")
            f.seek(peerinfo.size - 1);
            f.write("\0")
            f.flush()
            f.seek(0)
            fseekEndTime = time.time();
            print "--fseek  : elapsed = %f secs" % (fseekEndTime - fseekStartTime)

            endPointStartTime = time.time();
            with Endpoint(f.fileno(), peerinfo.size, dev) as end:
                with rdma.get_gmp_mad(end.ctx.end_port,verbs=end.ctx) as umad:
                    end.path = peerinfo.path;
                    end.path.end_port = end.ctx.end_port;
                    rdma.path.fill_path(end.qp,end.path);
                    rdma.path.resolve_path(umad,end.path);

                s.send(pickle.dumps(infotype(path=end.path,
                                             addr=end.mr.addr,
                                             rkey=end.mr.rkey,
                                             size=None,
                                             iters=None)))

                print "path to peer %r\nMR peer raddr=%x peer rkey=%x"%(
                    end.path,peerinfo.addr,peerinfo.rkey);

                end.connect(peerinfo)
                endPointEndTime = time.time();
                print "--endpoint: elapsed = %f secs" % (endPointEndTime - endPointStartTime)

                startTime = time.time();

                # Synchronize the transition to RTS
                s.send("ready");
                s.recv(1024);

                s.shutdown(socket.SHUT_WR);
                s.recv(1024);

                endTime = time.time();

                print "--xfer end: elapsed = %f secs" % (endTime - startTime)
                print "--total   : elapsed = %f secs" % (endTime - totalStartTime)
                #f = open(outfilename, "wb")
                #data = end.mem.read(peerinfo.size)
                #f.write(data)
                f.close()

def main():

    if len(sys.argv) < 2:
        print "Usage: [server] %s outputfilename" % sys.argv[0]
        print "       [client] %s ipaddr inputfile" % sys.argv[0]
        sys.exit(1)

    if len(sys.argv) == 3:
        client_mode(sys.argv[1], sys.argv[2], rdma.get_end_port())
    else:
        server_mode(sys.argv[1], rdma.get_end_port())
    return True;

main()

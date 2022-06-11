import pickle

import _pydctx

class Operation:
    def __init__(self, op, extract):
        self._op = op
        self._extract = extract

    def wait(self):
        result = self._op.wait()
        if result is None:
            return None
        if self._extract:
            return pickle.loads(result)
        return [pickle.loads(r) for r in result]


class DistributedContext:
    def __init__(
        self,
        rank,
        size,
        local_rank,
        local_size,
        cross_rank,
        cross_size,
        chief_host,
        chief_svc,
    ):
        self.rank = rank
        self.size = size
        self.local_rank = local_rank
        self.local_size = local_size
        self.cross_rank = cross_rank
        self.cross_size = cross_size

        self._dctx = _pydctx.DCTX(
            rank,
            size,
            local_rank,
            local_size,
            cross_rank,
            cross_size,
            chief_host,
            chief_svc,
        )

    def __enter__(self):
        self._dctx.__enter__()
        return self

    def __exit__(self, *args):
        self._dctx.__exit__(*args)

    def gather(self, data, series=""):
        op = self._dctx.gather(pickle.dumps(data), series)
        return Operation(op, extract=False)

    def broadcast(self, data, series=""):
        op = self._dctx.broadcast(pickle.dumps(data), series)
        return Operation(op, extract=True)

    def allgather(self, data, series=""):
        op = self._dctx.allgather(pickle.dumps(data), series)
        return Operation(op, extract=False)


# test code
if __name__ == "__main__":
    import contextlib

    with contextlib.ExitStack() as es:
        w0 = es.enter_context(
            DistributedContext(0,3, 0,0, 0,0, "localhost", "12345")
        )
        w1 = es.enter_context(
            DistributedContext(1,3, 0,0, 0,0, "localhost", "12345")
        )
        w2 = es.enter_context(
            DistributedContext(2,3, 0,0, 0,0, "localhost", "12345")
        )

        a2x = w2.allgather("ag2", series="x")

        g0x = w0.gather("chief", series="x")
        g1x = w1.gather("worker1", series="x")
        g2y = w2.gather("WORKER 2", series="y")

        b0x = w0.broadcast("bchief", series="x")

        a1x = w1.allgather("ag1", series="x")

        g0y = w0.gather("CHIEF", series="y")
        g1y = w1.gather("WORKER1", series="y")
        g2x = w2.gather("worker 2", series="x")

        b1x = w1.broadcast(None, series="x")
        b2x = w2.broadcast(None, series="x")

        a0x = w0.allgather("ag0", series="x")

        rg0x = g0x.wait();
        rg1x = g1x.wait();
        rg2x = g2x.wait();
        assert rg0x == ["chief", "worker1", "worker 2"], rg0x
        assert rg1x is None, rg1x
        assert rg2x is None, rg2x

        rg0y = g0y.wait();
        rg1y = g1y.wait();
        rg2y = g2y.wait();
        assert rg0y == ["CHIEF", "WORKER1", "WORKER 2"], rg0y
        assert rg1y is None, rg1y
        assert rg2y is None, rg2y

        rb0x = b0x.wait();
        rb1x = b1x.wait();
        rb2x = b2x.wait();
        assert rb0x == "bchief", rb0x
        assert rb1x == "bchief", rb1x
        assert rb2x == "bchief", rb2x

        ra0x = a0x.wait();
        ra1x = a1x.wait();
        ra2x = a2x.wait();
        assert ra0x == ["ag0", "ag1", "ag2"], ra0x
        assert ra1x == ["ag0", "ag1", "ag2"], ra1x
        assert ra2x == ["ag0", "ag1", "ag2"], ra2x

        print("PASS!")

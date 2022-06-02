
.PHONY: lib
lib:
	(cd dctx/build; ninja)

dctx/build/libdctx.so:
	(cd dctx/build; ninja)

.PHONY: run-gather
run-gather:
	LD_LIBRARY_PATH=`pwd`/dctx/build julia allgather.jl

.PHONY: run-context
run-context:
	LD_LIBRARY_PATH=`pwd`/dctx/build julia dctx.jl

docker: dctx/build/libdctx.so
	cp dctx/build/libdctx.so model/
	(cd model; docker build -t mackrorysd/julia-harness:latest -f Dockerfile .)

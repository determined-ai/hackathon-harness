build:
	(cd dctx/build; ninja)

run:
	LD_LIBRARY_PATH=`pwd`/dctx/build julia c.jl

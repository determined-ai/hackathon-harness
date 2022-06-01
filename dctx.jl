function dctx_open(rank, size, local_rank, local_size, cross_rank, cross_size, chief_host, chief_service)
    ctx = Ref{Ptr{Cvoid}}()
    ret = ccall((:dctx_open, "libdctx"), Int32,
        (Ptr{Cvoid}, Int32, Int32, Int32, Int32, Int32, Int32, Cstring, Cstring),
        ctx, rank, size, local_rank, local_size, cross_rank, cross_size, chief_host, chief_service
    )
    if ret != 0
        error("non-zero status returned")
    end
    return ctx
end

function dctx_close(ctx)
    ccall((:dctx_close, "libdctx"), Cvoid, (Ptr{Cvoid},), ctx)
end

chief = dctx_open(0, 2, 0, 2, 0, 1, "localhost", "1234")
worker = dctx_open(1, 2, 1, 2, 0, 1, "localhost", "1234")
sleep(1)
dctx_close(chief)
dctx_close(worker)

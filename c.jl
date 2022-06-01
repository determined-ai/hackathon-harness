function dctx_open(rank, size, local_rank, local_size, cross_rank, cross_size, chief_addr, len)
    ctx = Ref{Ptr{Cvoid}}()
    ret = ccall((:dctx_open, "libdctx"), Int32,
        (Ptr{Cvoid}, Int32, Int32, Int32, Int32, Int32, Int32, Cstring, Csize_t),
        ctx, rank, size, local_rank, local_size, cross_rank, cross_size, chief_addr, len
    )
    if ret != 0
        error("non-zero status returned")
    end
    return ctx
end

function dctx_close(ctx)
    ccall((:dctx_close, "libdctx"), Cvoid, (Ptr{Cvoid},), ctx)
end

ctx = dctx_open(0, 2, 0, 2, 0, 1, "localhost:1234", 0)
dctx_close(ctx)

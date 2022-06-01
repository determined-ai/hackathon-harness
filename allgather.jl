function dctx_gather_start(ctx, data, len)
    ccall((:dctx_gather_start, "libdctx"), Cint,
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t),
        ctx, ser, len)
end

function dctx_gather_end(ctx)
    ccall((:dctx_gather_end, "libdctx"), Ptr{Cvoid}, (Ptr{Cvoid},), ctx)
end

function dc_result_ok(dc_result)
    ccall((:dc_result_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_result)
end

function dc_result_count(dc_result)
    ccall((:dc_result_count, "libdctx"), Csize_t, (Ptr{Cvoid},), dc_result)
end

function dc_result_take(dc_result, i)
    len = 0
    data = ccall((:dc_result_take, "libdctx"), Ptr{UInt8}, (Ptr{Cvoid}, Csize_t, Ptr{Csize_t}), dc_result, i, len)
    return data, len
end

function gather(ctx, data)
    buf = IOBuffer()
    serialize(buf, data)
    ser = takebuf_string(io)
    status = dctx_gather_start(ctx, ser, lastindex(ser))
    if status != 0
        error("failed to start gather")
    end
    result = dctx_gather_end(ctx)
    if !dc_result_ok(result)
        error("distributed result was not ok")
    end
    count = dc_result_count(result)
    results = []
    for i in 1:count
        bytes, len = dc_result_take(i-1)
        X = unsafe_wrap(Array{UInt8}, Ptr{UInt8}(bytes), len)
        buf = IOBuffer()
        write(buf, ser)
        ccall(:free, Cvoid, (Ptr{Cvoid},), ser)
        push!(results, deserialize(buf))
    end
    return results
end

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

chief   = dctx_open(0, 2, 0, 2, 0, 1, "localhost", "1234")
worker1 = dctx_open(1, 2, 1, 2, 0, 1, "localhost", "1234")
worker2 = dctx_open(1, 2, 1, 2, 0, 1, "localhost", "1234")
sleep(1)
dctx_close(chief)
dctx_close(worker)

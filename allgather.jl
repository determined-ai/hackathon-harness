function dctx_gather(ctx, data, len)
    result = ccall((:dctx_gather, "libdctx"), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t),
        ctx, ser, len)
    return result
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

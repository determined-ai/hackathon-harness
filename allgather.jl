function dctx_allgather(ctx, data, len)
    result = ccall((:dctx_allgather, "libdctx"), Ptr{Cvoid},
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

function allgather(ctx, data)
    buf = IOBuffer()
    serialize(buf, data)
    ser = takebuf_string(io)
    result = dctx_allgather(ser, lastindex(ser))
    if !dc_result_ok(result)
        error("distributed result was not ok")
    end
    count = dc_result_count(result)
    results = []
    for i in 1:count
        ser, len = dc_result_take(i-1)
        # TODO use the length and pointer to create a Julia byte array
        buf = IOBuffer()
        write(buf, ser)
        ccall(:free, Cvoid, (Ptr{Cvoid},), ser)
        push!(results, deserialize(buf))
    end
    return results
end

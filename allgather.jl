using Printf

function dctx_gather_start(ctx, data, len)
    ccall((:dctx_gather_start, "libdctx"), Cint,
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t),
        ctx, data, len)
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

function free(ptr)
    #ccall(:free, Cvoid, (Ptr{Cvoid},), ptr)
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

chief   = dctx_open(0, 3, 0, 3, 0, 1, "0.0.0.0", "1234")
worker1 = dctx_open(1, 3, 1, 3, 0, 1, "localhost", "1234")
worker2 = dctx_open(2, 3, 2, 3, 0, 1, "localhost", "1234")

sleep(1)

chief_string = "chief"
worker1_string = "worker1"
worker2_string = "worker 2"

x = dctx_gather_start(chief, pointer_from_objref(chief_string), 5) 
@printf("%d\n", x)
if x != 0
    error("non-zero returned by dctx_gather_start on chief")
end

if dctx_gather_start(worker1, pointer_from_objref(worker1_string), 7) != 0
    error("non-zero returned by dctx_gather_start on worker1")
end

if dctx_gather_start(worker2, pointer_from_objref(worker2_string), 8) != 0
    error("non-zero returned by dctx_gather_start on worker2")
end

c = dctx_gather_end(chief)
w1 = dctx_gather_end(worker1)
w2 = dctx_gather_end(worker2)

if !dc_result_ok(c)
    error("result not ok on chief")
end

if !dc_result_ok(w1)
    error("result not ok on worker1")
end

if !dc_result_ok(w2)
    error("result not ok on worker2")
end

if dc_result_count(w1) != 0
    error("expected zero result on worker 1")
end

if dc_result_count(w2) != 0
    error("expected zero result on worker 2")
end

data, len = dc_result_take(c, 0)
if len != 5 or data != "chief"
    error("Unexpected first result on chief")
    free(data)
end
data, len = dc_result_take(c, 1);
if len != 7 or data != "worker1"
    error("Unexpected second result on chief")
    free(data)
end
if len != 8 or data != "worker 2"
    error("Unexpected third result on chief")
    free(data)
end

dctx_close(chief)
dctx_close(worker)

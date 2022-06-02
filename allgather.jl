using Printf

# int dctx_gather_start(struct dctx *dctx, char *data, size_t len);
function dctx_gather_start(ctx, data)
    status = ccall((:dctx_gather_start, "libdctx"), Cint,
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t),
        ctx, data, length(data))
    if status != 0
        error("dctx_gather_start failed")
    end
end

# struct dc_result *dctx_gather_end(struct dctx *dctx);
function dctx_gather_end(ctx)
    dc_result = ccall((:dctx_gather_end, "libdctx"), Ptr{Cvoid}, (Ptr{Cvoid},), ctx)

    # check status
    ok = ccall((:dc_result_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_result)
    if ok != 1
        error("distributed result was not ok")
    end

    count = ccall((:dc_result_count, "libdctx"), Csize_t, (Ptr{Cvoid},), dc_result)

    # allocate vector output
    out = Vector{String}(undef, count)

    # Set each output
    for i = 1:count
        len = Ref(UInt64(0))
        data = ccall((:dc_result_take, "libdctx"), Ptr{UInt8},
                           (Ptr{Cvoid}, Csize_t, Ptr{Csize_t}),
                           dc_result, i-1, len)
        out[i] = unsafe_string(data, len[])
    end

    # TODO: dc_result_free

    return out
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
    ctx = ccall((:dctx_open2, "libdctx"), Ptr{Cvoid},
        (Int32, Int32, Int32, Int32, Int32, Int32, Cstring, Cstring),
        rank, size, local_rank, local_size, cross_rank, cross_size, chief_host, chief_service
    )
    if ctx == 0
        error("dctx not returned")
    end
    return ctx
end

function dctx_close(ctx)
    ccall((:dctx_close2, "libdctx"), Cvoid, (Ptr{Cvoid},), ctx)
end

chief   = dctx_open(0, 3, 0, 3, 0, 1, "localhost", "1234")
worker1 = dctx_open(1, 3, 1, 3, 0, 1, "localhost", "1234")
worker2 = dctx_open(2, 3, 2, 3, 0, 1, "localhost", "1234")

sleep(1)

chief_string = "chief"
worker1_string = "worker1"
worker2_string = "worker 2"

dctx_gather_start(chief, chief_string)
dctx_gather_start(worker1, worker1_string)
dctx_gather_start(worker2, worker2_string)

@printf("sleeping\n");

sleep(1)

@printf("done sleeping\n");

c = dctx_gather_end(chief)
w1 = dctx_gather_end(worker1)
w2 = dctx_gather_end(worker2)

print(c)
print("\n")
print(w1)
print("\n")
print(w2)
print("\n")

dctx_close(chief)
dctx_close(worker1)
dctx_close(worker2)

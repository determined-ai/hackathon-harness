using Printf
using Serialization

struct DistributedContext
    # the C library object
    _dctx::Ptr{Cvoid}
    rank::Int
    size::Int
    local_rank::Int
    local_size::Int
    cross_rank::Int
    cross_size::Int

    # inner constructor
    DistributedContext(
        rank::Int,
        size::Int,
        local_rank::Int,
        local_size::Int,
        cross_rank::Int,
        cross_size::Int,
        chief_host::String,
        chief_service::String,
    ) = begin
        # call the C code to instantiate the thing
        dctx = ccall(
            (:dctx_open2, "libdctx"),
            Ptr{Cvoid},
            (Int32, Int32, Int32, Int32, Int32, Int32, Cstring, Cstring),
            rank,
            size,
            local_rank,
            local_size,
            cross_rank,
            cross_size,
            chief_host,
            chief_service,
        )
        if dctx == 0
            error("dctx not returned")
        end

        new(dctx, rank, size, local_rank, local_size, cross_rank, cross_size)
    end
end

mutable struct DistributedOperation
    # the C library object
    _op::Ptr{Cvoid}

    # hold a reference to the bytes we pass into dctx_gather_start_nofree, so
    # they aren't GC'd before it is sent over the wire
    _preserve::Ref
end

close(dctx::DistributedContext) = begin
    ccall((:dctx_close2, "libdctx"), Cvoid, (Ptr{Cvoid},), dctx._dctx)
end

gather_start(dctx::DistributedContext, obj::Any; series::String = "") = begin
    # serialize the object
    io = IOBuffer()
    serialize(io, obj)
    # get the content of the buffer (without a copy)
    data = take!(io)
    # call dctx_gather_start with the serialized data
    dc_op = ccall((:dctx_gather_start_nofree, "libdctx"), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
        dctx._dctx, series, length(series), data, length(data))
    ok = ccall((:dc_op_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_op)
    if ok != 1
        error("dctx_gather_start failed")
    end

    DistributedOperation(dc_op, Ref(data))
end

await(dc_op::DistributedOperation) = begin
    # use @threadcall intead of ccall to avoid blocking the rest of julia
    dc_result = @threadcall(
        (:dc_op_await, "libdctx"), Ptr{Cvoid}, (Ptr{Cvoid},), dc_op._op
    )
    try
        # done preserving the buffer
        dc_op._preserve = Ref(0)

        ok = ccall(
            (:dc_result_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_result
        )
        if ok != 1
            error("distributed result was not ok")
        end

        count = ccall(
            (:dc_result_count, "libdctx"), Csize_t, (Ptr{Cvoid},), dc_result
        )

        # allocate vector output
        out = Vector{Any}(undef, count)

        # Set each output
        for i = 1:count
            len = ccall(
                (:dc_result_len, "libdctx"),
                Csize_t,
                (Ptr{Cvoid}, Csize_t),
                dc_result,
                i-1
            )
            data = ccall(
                (:dc_result_take, "libdctx"),
                Ptr{UInt8},
                (Ptr{Cvoid}, Csize_t),
                dc_result,
                i-1
            )
            # tell julia to take ownership of the buffer, as a UInt8
            # (this will cause julia to call free(data) eventually)
            jldata = unsafe_wrap(Array{UInt8}, data, len[], own=true)
            # create a read-only IOBuffer around the array
            io = IOBuffer(jldata, read=true, write=false, append=false)
            # deserialize the data we got
            obj = deserialize(io)
            out[i] = obj
        end

        return out

    finally
        ccall((:dc_result_free2, "libdctx"), Cvoid, (Ptr{Cvoid},), dc_result)
    end
end

gather(dctx::DistributedContext, obj::Any) = begin
    await(gather_start(dctx, obj))
end

chief = DistributedContext(0, 3, 0, 0, 0, 0, "localhost", "1234")
worker1 = DistributedContext(1, 3, 0, 0, 0, 0, "localhost", "1234")
worker2 = DistributedContext(2, 3, 0, 0, 0, 0, "localhost", "1234")

# worker 2 will start gathers in opposite order

gca = gather_start(chief, "chief", series="a")
gw1a = gather_start(worker1, "worker1", series="a")
gw2b = gather_start(worker2, 2, series="b")

gcb = gather_start(chief, "CHIEF", series="b")
gw1b = gather_start(worker1, "WORKER1", series="b")
gw2a = gather_start(worker2, "two", series="a")

ca = await(gca)
w1a = await(gw1a)
w2a = await(gw2a)

cb = await(gcb)
w1b = await(gw1b)
w2b = await(gw2b)

print("series=a: ")
print(ca)
print("\n")

if ca != ["chief", "worker1", "two"]
    error("chief gathered wrong")
end

print("series=b: ")
print(cb)
print("\n")

if ca != ["chief", "worker1", "two"]
    error("chief gathered wrong")
end

if !(w1a == w2a == w1b == w2b == [])
    error("workers had non-empty gathers")
end

close(chief)
close(worker1)
close(worker2)

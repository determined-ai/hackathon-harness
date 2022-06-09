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

    _extract_result::Bool

    # hold a reference to the bytes we pass into dctx_gather_nofree, so
    # they aren't GC'd before it is sent over the wire
    _preserve::Ref
end

close(dctx::DistributedContext) = begin
    ccall((:dctx_close2, "libdctx"), Cvoid, (Ptr{Cvoid},), dctx._dctx)
end

gather(dctx::DistributedContext, obj::Any; series::String = "") = begin
    # serialize the object
    io = IOBuffer()
    serialize(io, obj)
    # get the content of the buffer (without a copy)
    data = take!(io)
    # call dctx_gather with the serialized data
    dc_op = ccall((:dctx_gather_nofree, "libdctx"), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
        dctx._dctx, series, length(series), data, length(data))
    ok = ccall((:dc_op_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_op)
    if ok != 1
        error("dctx_gather failed")
    end

    DistributedOperation(dc_op, false, Ref(data))
end

broadcast(dctx::DistributedContext, obj::Any; series::String = "") = begin
    data::Vector{UInt8} = []
    if dctx.rank == 0
        # serialize the object
        io = IOBuffer()
        serialize(io, obj)
        # get the content of the buffer (without a copy)
        data = take!(io)
    end
    # call dctx_broadcast with the serialized data
    dc_op = ccall((:dctx_broadcast_copy, "libdctx"), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
        dctx._dctx, series, length(series), data, length(data))
    ok = ccall((:dc_op_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_op)
    if ok != 1
        error("dctx_broadcast failed")
    end

    # no need to keep a ref for broadcast, since chief always copies
    DistributedOperation(dc_op, true, Ref(0))
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

        if dc_op._extract_result
            if length(out) != 1
                error("broadcast returned other-than-one element")
            end
            return out[1]
        end

        return out

    finally
        ccall((:dc_result_free2, "libdctx"), Cvoid, (Ptr{Cvoid},), dc_result)
    end
end

gather(dctx::DistributedContext, obj::Any) = begin
    await(gather(dctx, obj))
end

chief = DistributedContext(0, 3, 0, 0, 0, 0, "localhost", "1234")
worker1 = DistributedContext(1, 3, 0, 0, 0, 0, "localhost", "1234")
worker2 = DistributedContext(2, 3, 0, 0, 0, 0, "localhost", "1234")

# worker 2 will start gathers in opposite order

g0a = gather(chief, "chief", series="a")
g1a = gather(worker1, "worker1", series="a")
g2b = gather(worker2, 2, series="b")

b0a = broadcast(chief, "bchief", series="a")

g0b = gather(chief, "CHIEF", series="b")
g1b = gather(worker1, "WORKER1", series="b")
g2a = gather(worker2, "two", series="a")

b1a = broadcast(worker1, "", series="a")
b2a = broadcast(worker2, "", series="a")

rg0a = await(g0a)
rg1a = await(g1a)
rg2a = await(g2a)

rg0b = await(g0b)
rg1b = await(g1b)
rg2b = await(g2b)

println("gather(series=a): ", rg0a)

if rg0a != ["chief", "worker1", "two"]
    error("chief gathered wrong")
end

println("gather(series=b): ", rg0b)

if rg0b != ["CHIEF", "WORKER1", 2]
    error("chief gathered wrong")
end

if !(rg1a == rg2a == rg1b == rg2b == [])
    error("workers had non-empty gathers")
end

rb0a = await(b0a)
rb1a = await(b1a)
rb2a = await(b2a)

println("broadcast(series=b): ", rb0a, ", ", rb1a, ", ", rb2a)

if !(rb0a == rb1a == rb2a == "bchief")
    error("broadcast was wrong")
end

close(chief)
close(worker1)
close(worker2)

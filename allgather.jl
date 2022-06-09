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

allgather(dctx::DistributedContext, obj::Any; series::String = "") = begin
    # serialize the object
    io = IOBuffer()
    serialize(io, obj)
    # get the content of the buffer (without a copy)
    data = take!(io)

    # call dctx_allgather with the serialized data
    dc_op = ccall((:dctx_allgather_nofree, "libdctx"), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
        dctx._dctx, series, length(series), data, length(data))
    ok = ccall((:dc_op_ok, "libdctx"), Cuchar, (Ptr{Cvoid},), dc_op)
    if ok != 1
        error("dctx_broadcast failed")
    end

    DistributedOperation(dc_op, false, Ref(data))
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

# submit ops in arbitrary order

a2x = allgather(worker2, "ag2", series="x")

g0x = gather(chief, "chief", series="x")
g1x = gather(worker1, "worker1", series="x")
g2y = gather(worker2, 2, series="y")

b0x = broadcast(chief, "bchief", series="x")

a1x = allgather(worker1, "ag1", series="x")

g0y = gather(chief, "CHIEF", series="y")
g1y = gather(worker1, "WORKER1", series="y")
g2x = gather(worker2, "two", series="x")

b1x = broadcast(worker1, "", series="x")
b2x = broadcast(worker2, "", series="x")

a0x = allgather(chief, "ag0", series="x")

rg0x = await(g0x)
rg1x = await(g1x)
rg2x = await(g2x)

rg0y = await(g0y)
rg1y = await(g1y)
rg2y = await(g2y)

rb0x = await(b0x)
rb1x = await(b1x)
rb2x = await(b2x)

ra0x = await(a0x)
ra1x = await(a1x)
ra2x = await(a2x)

println("gather(series=x): ", rg0x)

if rg0x != ["chief", "worker1", "two"]
    error("chief gathered wrong")
end

println("gather(series=y): ", rg0y)

if rg0y != ["CHIEF", "WORKER1", 2]
    error("chief gathered wrong")
end

if !(rg1x == rg2x == rg1y == rg2y == [])
    error("workers had non-empty gathers")
end

println("broadcast(series=x): ", rb0x, ", ", rb1x, ", ", rb2x)

if !(rb0x == rb1x == rb2x == "bchief")
    error("broadcast was wrong")
end

println("allgather(series=x): ", ra0x, ", ", ra1x, ", ", ra2x)

if !(ra0x == ra1x == ra2x == ["ag0", "ag1", "ag2"])
    error("allgather was wrong")
end

close(chief)
close(worker1)
close(worker2)

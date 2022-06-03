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

mutable struct DistributedGather
    # the C library object
    _dctx::Ptr{Cvoid}

    # hold a reference to the bytes we pass into dctx_gather_start_nofree, so
    # they aren't GC'd before it is sent over the wire
    _preserve::Ref
end

close(dctx::DistributedContext) = begin
    ccall((:dctx_close2, "libdctx"), Cvoid, (Ptr{Cvoid},), dctx._dctx)
end

gather_start(dctx::DistributedContext, obj::Any) = begin
    # serialize the object
    io = IOBuffer()
    serialize(io, obj)
    # get the content of the buffer (without a copy)
    data = take!(io)
    # call dctx_gather_start with the serialized data
    status = ccall((:dctx_gather_start_nofree, "libdctx"), Cint,
        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t),
        dctx._dctx, data, length(data))
    if status != 0
        error("dctx_gather_start failed")
    end

    DistributedGather(dctx._dctx, Ref(data))
end

gather_end(dctx::DistributedGather) = begin
    # use @threadcall intead of ccall to avoid blocking the rest of julia
    dc_result = @threadcall(
        (:dctx_gather_end, "libdctx"), Ptr{Cvoid}, (Ptr{Cvoid},), dctx._dctx
    )
    try
        # done preserving the buffer
        dctx._preserve = Ref(0)

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
            len = Ref(UInt64(0))
            data = ccall((:dc_result_take, "libdctx"), Ptr{UInt8},
                               (Ptr{Cvoid}, Csize_t, Ptr{Csize_t}),
                               dc_result, i-1, len)
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
    gather_end(gather_start(dctx, obj))
end

chief = DistributedContext(0, 3, 0, 0, 0, 0, "localhost", "1234")
worker1 = DistributedContext(1, 3, 0, 0, 0, 0, "localhost", "1234")
worker2 = DistributedContext(2, 3, 0, 0, 0, 0, "localhost", "1234")

gc = gather_start(chief, "chief")
gw1 = gather_start(worker1, "worker1")
gw2 = gather_start(worker2, 2)

c = gather_end(gc)
w1 = gather_end(gw1)
w2 = gather_end(gw2)

print(c)
print("\n")
print(w1)
print("\n")
print(w2)
print("\n")

close(chief)
close(worker1)
close(worker2)

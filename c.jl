y = ccall((:hello, "libdctx"), Int32, (Cstring,), "World!")
println(y)

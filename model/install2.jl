using CUDA
using Flux
using Pkg
using AWS
using AWSS3

# Trigger CUDA artifact installation.
Dense(1,1) |> gpu
Pkg.add("IJulia")

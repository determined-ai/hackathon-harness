using CUDA
using Flux
using Pkg

# Trigger CUDA artifact installation.
Dense(1,1) |> gpu
Pkg.add("IJulia")
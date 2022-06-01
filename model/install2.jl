using CUDA
using Flux
using Pkg

Pkg.add("AWS")
Pkg.add("AWSS3")
Pkg.add("BSON")

# Trigger CUDA artifact installation.
Dense(1,1) |> gpu
Pkg.add("IJulia")

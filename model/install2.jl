using CUDA
using Flux
using Pkg

# Trigger CUDA artifact installation.
Dense(1,1) |> gpu
Pkg.add("IJulia")

Pkg.add("AWS")
Pkg.add("AWSS3")
Pkg.add("BSON")
Pkg.add("Plots")
Pkg.add("Images")

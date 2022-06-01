using CUDA
using Flux

# Trigger CUDA artifact installation.
Dense(1,1) |> gpu
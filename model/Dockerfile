FROM determinedai/environments:cuda-11.3-pytorch-1.10-lightning-1.5-tf-2.8-gpu-3e933ea
RUN wget -q https://julialang-s3.julialang.org/bin/linux/x64/1.7/julia-1.7.3-linux-x86_64.tar.gz
RUN tar xfz julia-1.7.3-linux-x86_64.tar.gz
RUN mv julia-1.7.3 /opt/julia-1.7.3
RUN ln -s /opt/julia-1.7.3/bin/julia /usr/local/bin/julia
COPY install.jl install.jl
RUN echo "import Pkg" > echo.jl
RUN julia install.jl
COPY libdctx.so /usr/lib/
RUN julia install2.jl
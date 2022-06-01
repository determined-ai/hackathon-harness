wget -q https://julialang-s3.julialang.org/bin/linux/x64/1.7/julia-1.7.3-linux-x86_64.tar.gz
tar xfz julia-1.7.3-linux-x86_64.tar.gz
mv julia-1.7.3 /opt/julia-1.7.3
ln -s /opt/julia-1.7.3/bin/julia /usr/local/bin/julia
julia install-http.jl
using AWSS3
using AWS # for `global_aws_config`
using Flux
using Flux: loadmodel!
using BSON
using BSON: @save


p = S3Path("s3://det-no-py-harness-hackathon-us-west-2-573932760021/model.txt")  # provides an filesystem-like interface
aws = global_aws_config(;region="us-west-2") # pass keyword arguments to change defaults


model = Chain(Dense(10 => 5,relu),Dense(5 => 2),softmax)

@save "model.bson" model
data = read("model.bson")
s3_put(aws, "det-no-py-harness-hackathon-us-west-2-573932760021","model.bson", data)

weights = s3_get(aws, "det-no-py-harness-hackathon-us-west-2-573932760021", "model.bson")
open("checkpoint.bson", "w") do chk
   write(chk, weights)
end;

model = loadmodel!(model, BSON.load("checkpoint.bson")[:model])
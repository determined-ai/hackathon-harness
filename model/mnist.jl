using Flux, Statistics
using Flux.Data: DataLoader
using Flux: onehotbatch, onecold, @epochs
using Flux.Losses: logitcrossentropy
using Base: @kwdef
using CUDA
using MLDatasets
using HTTP
using JSON
using Random
using AWSS3
using AWS # for `global_aws_config`
using Flux: loadmodel!
using BSON
using BSON: @save

include("../logging.jl")

master_url = ENV["DET_MASTER"]
token = ENV["DET_SESSION_TOKEN"]
trial_run_id = ENV["DET_TRIAL_RUN_ID"]
trial_id = ENV["DET_TRIAL_ID"]
experiment_id = ENV["DET_EXPERIMENT_ID"]

hparams = JSON.parse(ENV["DET_HPARAMS"])

aws = global_aws_config(;region="us-west-2") # pass keyword arguments to change defaults

# Distributed computing primitives
function dctx_open(rank, size, local_rank, local_size, cross_rank, cross_size, chief_host, chief_service)
    ctx = Ref{Ptr{Cvoid}}()
    ret = ccall((:dctx_open, "libdctx"), Int32,
        (Ptr{Cvoid}, Int32, Int32, Int32, Int32, Int32, Int32, Cstring, Cstring),
        ctx, rank, size, local_rank, local_size, cross_rank, cross_size, chief_host, chief_service
    )
    if ret != 0
        error("non-zero status returned")
    end
    return ctx
end

function dctx_close(ctx)
    ccall((:dctx_close, "libdctx"), Cvoid, (Ptr{Cvoid},), ctx)
end

rank = parse(Int,ENV["RANK"])
size = parse(Int,ENV["SIZE"])
chiefIP = ENV["CHIEF_IP"]
ctx = dctx_open(rank, size, rank, size, 0, 1, chiefIP, "1234")
sleep(1)
dctx_close(ctx)

@debug "hparams[increment_by] = " * string(hparams["increment_by"])

function report_training_metrics(steps_completed, metrics)
    @info "report_training_metrics(steps_completed=$steps_completed, metrics=$metrics)"
    body = Dict(
        "trial_run_id" => trial_run_id,
        "steps_completed" => steps_completed,
        "metrics" => metrics
    ) 
    r = HTTP.request(
        "POST", 
        "$(master_url)/api/v1/trials/$(trial_id)/training_metrics", 
        headers, 
        body=JSON.json(body)
    )
end

function report_validation_metrics(steps_completed, metrics)
    @info "report_validation_metrics(steps_completed=$steps_completed, metrics=$metrics)"
    body = Dict(
        "trial_run_id" => trial_run_id,
        "steps_completed" => steps_completed,
        "metrics" => metrics
    ) 
    r = HTTP.request(
        "POST", 
        "$(master_url)/api/v1/trials/$(trial_id)/validation_metrics", 
        headers, 
        body=JSON.json(body)
    ) 
end

headers = Dict("Grpc-Metadata-x-allocation-token" => "Bearer $(token)")


# We set default values for learning rate, batch size, epochs, and the usage of a GPU (if available) for the model:

@kwdef mutable struct Args
    η::Float64 = 3e-4       ## learning rate
    batchsize::Int = 256    ## batch size
    epochs::Int = 10        ## number of epochs
    use_cuda::Bool = true   ## use gpu (if cuda available)
end

# If a GPU is available on our local system, then Flux uses it for computing the loss and updating the weights and biases when training our model.


# ## Data

# We create the function `getdata` to load the MNIST train and test data from [MLDatasets](https://github.com/JuliaML/MLDatasets.jl) and reshape them so that they are in the shape that Flux expects. 

function getdata(args)
    ENV["DATADEPS_ALWAYS_ACCEPT"] = "true"

    ## Load dataset	
    xtrain, ytrain = MLDatasets.MNIST(:train)[:]
    xtest, ytest = MLDatasets.MNIST(:test)[:]
	
    ## Reshape input data to flatten each image into a linear array
    xtrain = Flux.flatten(xtrain)
    xtest = Flux.flatten(xtest)

    ## One-hot-encode the labels
    ytrain, ytest = onehotbatch(ytrain, 0:9), onehotbatch(ytest, 0:9)

    ## Create two DataLoader objects (mini-batch iterators)
    train_loader = DataLoader((xtrain, ytrain), batchsize=args.batchsize, shuffle=true)
    test_loader = DataLoader((xtest, ytest), batchsize=args.batchsize)

    return train_loader, test_loader
end

# The function `getdata` performs the following tasks:

# * **Loads MNIST dataset:** Loads the train and test set tensors. The shape of train data is `28x28x60000` and test data is `28X28X10000`. 
# * **Reshapes the train and test data:**  Uses the [flatten](https://fluxml.ai/Flux.jl/stable/models/layers/#Flux.flatten) function to reshape the train data set into a `784x60000` array and test data set into a `784x10000`. Notice that we reshape the data so that we can pass these as arguments for the input layer of our model (a simple MLP expects a vector as an input).
# * **One-hot encodes the train and test labels:** Creates a batch of one-hot vectors so we can pass the labels of the data as arguments for the loss function. For this example, we use the [logitcrossentropy](https://fluxml.ai/Flux.jl/stable/models/losses/#Flux.Losses.logitcrossentropy) function and it expects data to be one-hot encoded. 
# * **Creates mini-batches of data:** Creates two DataLoader objects (train and test) that handle data mini-batches of size `1024 ` (as defined above). We create these two objects so that we can pass the entire data set through the loss function at once when training our model. Also, it shuffles the data points during each iteration (`shuffle=true`).

# ## Model

# As we mentioned above, a MLP consist of *three* layers that are fully connected. For this example, we define our model with the following layers and dimensions: 

# * **Input:** It has `784` perceptrons (the MNIST image size is `28x28`). We flatten the train and test data so that we can pass them as arguments to this layer.
# * **Hidden:** It has `32` perceptrons that use the [relu](https://fluxml.ai/Flux.jl/stable/models/nnlib/#NNlib.relu) activation function.
# * **Output:** It has `10` perceptrons that output the model's prediction or probability that a digit is 0 to 9. 


# We define the model with the `build_model` function: 


function build_model(; imgsize=(28,28,1), nclasses=10)
    return Chain( Dense(prod(imgsize), 32, relu),
                  Dense(32, nclasses))
end

# Note that we use the functions [Dense](https://fluxml.ai/Flux.jl/stable/models/layers/#Flux.Dense) so that our model is *densely* (or fully) connected and [Chain](https://fluxml.ai/Flux.jl/stable/models/layers/#Flux.Chain) to chain the computation of the three layers.

# ## Loss function

# Now, we define the loss function `loss_and_accuracy`. It expects the following arguments:
# * ADataLoader object.
# * The `build_model` function we defined above.
# * A device object (in case we have a GPU available).

function loss_and_accuracy(data_loader, model, device)
    acc = 0
    ls = 0.0f0
    num = 0
    for (x, y) in data_loader
        x, y = device(x), device(y)
        ŷ = model(x)
        ls += logitcrossentropy(ŷ, y, agg=sum)
        acc += sum(onecold(ŷ) .== onecold(y)) ## Decode the output of the model
        num +=  size(x)[end]
    end
    return ls / num, acc / num
end

# This function iterates through the `dataloader` object in mini-batches and uses the function 
# [logitcrossentropy](https://fluxml.ai/Flux.jl/stable/models/losses/#Flux.Losses.logitcrossentropy) to compute the difference between 
# the predicted and actual values (loss) and the accuracy. 


# ## Train function

# Now, we define the `train` function that calls the functions defined above and trains the model.

function train(; kws...)
    args = Args(; kws...) ## Collect options in a struct for convenience

    if CUDA.functional() && args.use_cuda
        @info "Training on CUDA GPU"
        CUDA.allowscalar(false)
        device = gpu
    else
        @info "Training on CPU"
        device = cpu
    end

    ## Create test and train dataloaders
    train_loader, test_loader = getdata(args)

    ## Construct model
    model = build_model() |> device
    ps = Flux.params(model) ## model's trainable parameters
    
    ## Optimizer
    opt = ADAM(hparams["lr"])
    
    ## Training
    for epoch in 1:args.epochs
        for (x, y) in train_loader
            x, y = device(x), device(y) ## transfer data to device
            gs = gradient(() -> logitcrossentropy(model(x), y), ps) ## compute gradient
            Flux.Optimise.update!(opt, ps, gs) ## update parameters
        end
        
        ## Report on train and test
        train_loss, train_acc = loss_and_accuracy(train_loader, model, device)
        test_loss, test_acc = loss_and_accuracy(test_loader, model, device)
        validation_metrics = Dict("validation_accuracy" => test_acc)
        training_metrics = Dict("training_accuracy" => train_acc)
        report_training_metrics(epoch, training_metrics)
        report_validation_metrics(epoch, validation_metrics)        
        @info "Epoch=$epoch"
        @info "  train_loss = $train_loss, train_accuracy = $train_acc"
        @info "  test_loss = $test_loss, test_accuracy = $test_acc"
    end

    filename = "model-$(hparams["lr"]).bson"
    @save filename model
    data = read(filename)
    s3_put(aws, "det-no-py-harness-hackathon-us-west-2-573932760021", filename, data)
end

# ## Run the example 

# We call the `train` function:

if abspath(PROGRAM_FILE) == @__FILE__
    train()
end

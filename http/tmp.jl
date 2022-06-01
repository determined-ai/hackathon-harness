using DiffEqFlux, DifferentialEquations, Printf
using Flux.Losses: logitcrossentropy
using Flux.Data: DataLoader
using MLDatasets
using MLDataUtils:  LabelEnc, convertlabel, stratifiedobs
using CUDA
using JSON
using HTTP
CUDA.allowscalar(false)

####

# trial run id :env variable
# DET_MASTER
# DET_TRIAL_RUN_ID
# DEFAULT_CLUSTER_INFO_PATH
## python

# bindings go here
function report_validation_metrics(
    steps_completed,
    metrics
):
    body = {
        "trial_run_id": self._run_id,
        "steps_completed": steps_completed,
        "metrics": metrics,
    }

    token =os.environ["DET_SESSION_TOKEN"],
  
    headers ={ "Grpc-Metadata-x-allocation-token": "Bearer {}".format(allocation_token)}
    post(
        f"/api/v1/trials/{self._trial_id}/validation_metrics",
        data=det.util.json_encode(body),
    )

    r = HTTP.request("POST", "/api/v1/trials/{self._trial_id}/validation_metrics",
        ["Content-Type" => "application/json"],
        JSON.json(params))


# def report_training_metrics(
#     self,
#     steps_completed: int,
#     metrics: Dict[str, Any],
#     batch_metrics: Optional[List[Dict[str, Any]]] = None,
# ) -> None:
#     logger.info(
#         f"report_training_metrics(steps_completed={steps_completed}, metrics={metrics})"
#     )
#     logger.debug(
#         f"report_training_metrics(steps_completed={steps_completed},"
#         f" batch_metrics={batch_metrics})"
#     )

# def report_validation_metrics(self, steps_completed: int, metrics: Dict[str, Any]) -> None:
#     serializable_metrics = self._get_serializable_metrics(metrics)
#     metrics = {k: metrics[k] for k in serializable_metrics}
#     logger.info(
#         f"report_validation_metrics(steps_completed={steps_completed} metrics={metrics})"
#     )

# def report_early_exit(self, reason: EarlyExitReason) -> None:
#     logger.info(f"report_early_exit({reason})")

# def get_experiment_best_validation(self) -> Optional[float]:
#     return None

#####



function loadmnist(batchsize = bs, train_split = 0.9)
    # Use MLDataUtils LabelEnc for natural onehot conversion
    onehot(labels_raw) = convertlabel(LabelEnc.OneOfK, labels_raw,
                                      LabelEnc.NativeLabels(collect(0:9)))
    # Load MNIST
    imgs, labels_raw = MNIST.traindata();
    # Process images into (H,W,C,BS) batches
    x_data = Float32.(reshape(imgs, size(imgs,1), size(imgs,2), 1, size(imgs,3)))
    y_data = onehot(labels_raw)
    (x_train, y_train), (x_test, y_test) = stratifiedobs((x_data, y_data),
                                                         p = train_split)
    return (
        # Use Flux's DataLoader to automatically minibatch and shuffle the data
        DataLoader(gpu.(collect.((x_train, y_train))); batchsize = batchsize,
                   shuffle = true),
        # Don't shuffle the test data
        DataLoader(gpu.(collect.((x_test, y_test))); batchsize = batchsize,
                   shuffle = false)
    )
end

# Main
const bs = 128
const train_split = 0.9
train_dataloader, test_dataloader = loadmnist(bs, train_split);

down = Chain(Conv((3, 3), 1=>64, relu, stride = 1), GroupNorm(64, 64),
             Conv((4, 4), 64=>64, relu, stride = 2, pad=1), GroupNorm(64, 64),
             Conv((4, 4), 64=>64, stride = 2, pad = 1)) |>gpu

dudt = Chain(Conv((3, 3), 64=>64, tanh, stride=1, pad=1),
             Conv((3, 3), 64=>64, tanh, stride=1, pad=1)) |>gpu

fc = Chain(GroupNorm(64, 64), x -> relu.(x), MeanPool((6, 6)),
           x -> reshape(x, (64, :)), Dense(64,10)) |> gpu
          
nn_ode = NeuralODE(dudt, (0.f0, 1.f0), Tsit5(),
                   save_everystep = false,
                   reltol = 1e-3, abstol = 1e-3,
                   save_start = false) |> gpu

function DiffEqArray_to_Array(x)
    xarr = gpu(x)
    return xarr[:,:,:,:,1]
end

# Build our over-all model topology
model = Chain(down,                 # (28, 28, 1, BS) -> (6, 6, 64, BS)
              nn_ode,               # (6, 6, 64, BS) -> (6, 6, 64, BS, 1)
              DiffEqArray_to_Array, # (6, 6, 64, BS, 1) -> (6, 6, 64, BS)
              fc)                   # (6, 6, 64, BS) -> (10, BS)

# To understand the intermediate NN-ODE layer, we can examine it's dimensionality
img, lab = train_dataloader.data[1][:, :, :, 1:1], train_dataloader.data[2][:, 1:1]

x_d = down(img)

# We can see that we can compute the forward pass through the NN topology
# featuring an NNODE layer.
x_m = model(img)

classify(x) = argmax.(eachcol(x))

function accuracy(model, data; n_batches = 100)
    total_correct = 0
    total = 0
    for (i, (x, y)) in enumerate(data)
        # Only evaluate accuracy for n_batches
        i > n_batches && break
        target_class = classify(cpu(y))
        predicted_class = classify(cpu(model(x)))
        total_correct += sum(target_class .== predicted_class)
        total += length(target_class)
    end
    return total_correct / total
end

# burn in accuracy
accuracy(model, train_dataloader)

loss(x, y) = logitcrossentropy(model(x), y)

# burn in loss
loss(img, lab)

opt = ADAM(0.05)
iter = 0

cb() = begin
    global iter += 1
    # Monitor that the weights do infact update
    # Every 10 training iterations show accuracy
    if iter % 10 == 1
        train_accuracy = accuracy(model, train_dataloader) * 100
        test_accuracy = accuracy(model, test_dataloader;
                                 n_batches = length(test_dataloader)) * 100
        @printf("Iter: %3d || Train Accuracy: %2.3f || Test Accuracy: %2.3f\n",
                iter, train_accuracy, test_accuracy)
    end
end

Flux.train!(loss, Flux.params(down, nn_ode.p, fc), train_dataloader, opt, cb = cb)
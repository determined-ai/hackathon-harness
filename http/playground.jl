using HTTP
using JSON
using Random

master_url = ENV["DET_MASTER"]
token = ENV["DET_SESSION_TOKEN"]
trial_run_id = ENV["DET_TRIAL_RUN_ID"]
trial_id = ENV["DET_TRIAL_ID"]
experiment_id = ENV["DET_EXPERIMENT_ID"]

hparams = JSON.parse(ENV["DET_HPARAMS"])



# steps_completed = parse(Int64, ENV["STEPS_COMPLETED"])
steps_completed = 0




println(master_url)
println(hparams)
println(token)



function report_training_metrics(steps_completed, metrics)
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


for steps_completed in steps_completed:steps_completed + 5
    validation_metrics = Dict("validation_accuracy" => rand())
    training_metrics = Dict("training_accuracy" => rand())
    report_training_metrics(steps_completed, training_metrics)
    report_validation_metrics(steps_completed, validation_metrics)
end

ENV["STEPS_COMPLETED"]  =steps_completed + 5


# sleep(1000000000000.0)


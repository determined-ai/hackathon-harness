using HTTP
using JSON


# HTTP.request(method, url [, headers [, body]]
try
    master_url = ENV["DET_MASTER"]
    token = ENV["DET_SESSION_TOKEN"]
    trial_run_id = ENV["DET_TRIAL_RUN_ID"]
    trial_id = ENV["DET_TRIAL_ID"]
    experiment_id = ENV["DET_EXPERIMENT_ID"]
catch


println(master_url)
println(token)
headers = Dict("Grpc-Metadata-x-allocation-token" => "Bearer $(token)")

for steps_completed in 1:10
    body = Dict(
        "trial_run_id" => trial_run_id,
        "steps_completed" => steps_completed,
        "metrics" =>
            Dict("validation_loss" => 0.05714966608912578),
    )

    r = HTTP.request(
        "POST", 
        "$(master_url)/api/v1/trials/$(trial_id)/validation_metrics", 
        headers, 
        body=JSON.json(body)
    )

end

sleep(1,000,000,000,000)
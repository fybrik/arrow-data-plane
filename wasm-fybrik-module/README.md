Steps to run without development:
- apply the assets in `sample_assets` directory: `kubectl apply -f <ASSET_PATH>`
- apply the policy: 
    - `kubectl -n fybrik-system create configmap sample-policy --from-file=sample_assets/sample-policy-filter.rego`
    - `kubectl -n fybrik-system label configmap sample-policy openpolicyagent.org/policy=rego`
- apply the fybrikmodule: `kubectl apply -f wasm-fybrik-module.yaml -n fybrik-system`
- apply the fybrikapplication: `kubectl apply -f fybrikapplication.yaml`
- make port-forwarding to the relay pod `kubectl port-forward <POD_NAME> -n fybrik-blueprints 8000:12232 &`
- Go to the `flight-client` directory (`cd ../flight-client`) and run `./client.sh localhost 8000`
use anyhow::Result;
use cached::proc_macro::cached;
use oci_distribution::{ manifest, secrets::RegistryAuth, Client};

// The size of the cache is 5.
// Entries stay for no more than an hour.
// Only `Ok` results are cached.
#[cached(size=5, time=3600, result=true)]
pub fn cached_pull_wasm_module(username: String, password: String, reference: String) -> Result<Vec<u8>> {
    return pull_wasm_module(username, password, reference);
}

#[tokio::main(flavor = "current_thread")]
async fn pull_wasm_module(username: String, password: String, reference: String) -> Result<Vec<u8>> {
    let reference = reference.parse()?;
    let mut client = Client::default();
    let registry_auth = RegistryAuth::Basic(username.parse()?, password.parse()?);
    let img = client
        .pull(
            &reference,
            &registry_auth,
            vec![
                manifest::WASM_LAYER_MEDIA_TYPE,
                manifest::IMAGE_MANIFEST_MEDIA_TYPE,
                manifest::IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE,
            ],
        )
        .await?;

    println!("Downloaded {}", img.digest());

    let layer = img.layers.get(0).unwrap();
    return Ok(layer.data.clone());
}
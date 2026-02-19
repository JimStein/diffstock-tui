use anyhow::Result;

pub fn probe_directml_session(model_path: &std::path::Path) -> Result<()> {
    #[cfg(feature = "directml")]
    {
        use ort::execution_providers::DirectMLExecutionProvider;
        use ort::session::Session;

        let _session = Session::builder()?
            .with_execution_providers([DirectMLExecutionProvider::default().build()])?
            .commit_from_file(model_path)?;
        return Ok(());
    }

    #[cfg(not(feature = "directml"))]
    {
        let _ = model_path;
        anyhow::bail!(
            "This binary was built without 'directml' feature. Rebuild with: cargo run --features directml -- --webui --compute-backend directml"
        );
    }
}

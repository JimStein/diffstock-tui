use anyhow::Result;
use candle_nn::VarMap;
use std::path::Path;
use std::process::Command;
use tracing::{info, warn};

pub const SAFETENSORS_PATH: &str = "model_weights.safetensors";
pub const ONNX_PATH: &str = "model_weights.onnx";

pub fn save_best_model_artifacts(varmap: &VarMap, use_cuda: bool) -> Result<()> {
    let safetensors_path = crate::config::project_file_path(SAFETENSORS_PATH);
    let onnx_path = crate::config::project_file_path(ONNX_PATH);

    varmap.save(&safetensors_path)?;
    info!("Saved safetensors checkpoint: {}", safetensors_path.display());

    if use_cuda {
        if let Err(e) = try_export_onnx(&safetensors_path, &onnx_path) {
            warn!(
                "ONNX export step failed (non-fatal): {}. Training continues with safetensors only.",
                e
            );
        }
    }

    Ok(())
}

fn try_export_onnx(input_path: &Path, output_path: &Path) -> Result<()> {
    let exporter = std::env::var("DIFFSTOCK_ONNX_EXPORTER").unwrap_or_else(|_| "python".to_string());
    let script = std::env::var("DIFFSTOCK_ONNX_EXPORT_SCRIPT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| crate::config::project_file_path("tools/export_to_onnx.py"));

    if !script.exists() {
        warn!(
            "ONNX export script not found at '{}'. Skipping ONNX generation.",
            script.display()
        );
        return Ok(());
    }

    info!(
        "Exporting ONNX checkpoint to {} using {} {}",
        output_path.display(), exporter, script.display()
    );

    let status = Command::new(&exporter)
        .arg(&script)
        .arg("--input")
        .arg(input_path)
        .arg("--output")
        .arg(output_path)
        .status()?;

    if status.success() {
        info!("Saved ONNX checkpoint: {}", output_path.display());
        Ok(())
    } else {
        anyhow::bail!(
            "export command exited with non-zero status: {} {} -> code {:?}",
            exporter,
            script.display(),
            status.code()
        )
    }
}

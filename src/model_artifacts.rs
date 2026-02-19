use anyhow::Result;
use candle_nn::VarMap;
use std::path::Path;
use std::process::Command;
use tracing::{info, warn};

pub const SAFETENSORS_PATH: &str = "model_weights.safetensors";
pub const ONNX_PATH: &str = "model_weights.onnx";

pub fn save_best_model_artifacts(varmap: &VarMap, use_cuda: bool) -> Result<()> {
    varmap.save(SAFETENSORS_PATH)?;
    info!("Saved safetensors checkpoint: {}", SAFETENSORS_PATH);

    if use_cuda {
        if let Err(e) = try_export_onnx(SAFETENSORS_PATH, ONNX_PATH) {
            warn!(
                "ONNX export step failed (non-fatal): {}. Training continues with safetensors only.",
                e
            );
        }
    }

    Ok(())
}

fn try_export_onnx(input_path: &str, output_path: &str) -> Result<()> {
    let exporter = std::env::var("DIFFSTOCK_ONNX_EXPORTER").unwrap_or_else(|_| "python".to_string());
    let script = std::env::var("DIFFSTOCK_ONNX_EXPORT_SCRIPT")
        .unwrap_or_else(|_| "tools/export_to_onnx.py".to_string());

    if !Path::new(&script).exists() {
        warn!(
            "ONNX export script not found at '{}'. Skipping ONNX generation.",
            script
        );
        return Ok(());
    }

    info!(
        "Exporting ONNX checkpoint to {} using {} {}",
        output_path, exporter, script
    );

    let status = Command::new(&exporter)
        .arg(&script)
        .arg("--input")
        .arg(input_path)
        .arg("--output")
        .arg(output_path)
        .status()?;

    if status.success() {
        info!("Saved ONNX checkpoint: {}", output_path);
        Ok(())
    } else {
        anyhow::bail!(
            "export command exited with non-zero status: {} {} -> code {:?}",
            exporter,
            script,
            status.code()
        )
    }
}

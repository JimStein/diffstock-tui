use candle_core::{Tensor, Result, Device};
use crate::models::time_grad::EpsilonTheta;

/// Gaussian Diffusion process for probabilistic time-series forecasting.
/// Implements the forward diffusion (adding noise) and reverse diffusion (denoising) steps.
pub struct GaussianDiffusion {
    pub num_steps: usize,
    pub beta: Tensor,
    pub alpha: Tensor,
    pub alpha_bar: Tensor,
    pub sigma: Tensor,
    pub sqrt_one_minus_alpha_bar: Tensor,
}

impl GaussianDiffusion {
    /// Creates a new Gaussian Diffusion instance with a linear beta schedule.
    pub fn new(num_steps: usize, device: &Device) -> Result<Self> {
        let beta_start = 1e-4f32;
        let beta_end = 0.02f32;
        let betas = (0..num_steps).map(|i| {
            beta_start + (beta_end - beta_start) * (i as f32 / (num_steps - 1) as f32)
        }).collect::<Vec<f32>>();

        let beta = Tensor::new(betas.as_slice(), device)?;
        let alpha = (1.0 - &beta)?;
        
        let mut alpha_bar_vec = Vec::with_capacity(num_steps);
        let mut cum_prod = 1.0f32;
        for &b in &betas {
            let a = 1.0 - b;
            cum_prod *= a;
            alpha_bar_vec.push(cum_prod);
        }
        let alpha_bar = Tensor::new(alpha_bar_vec.as_slice(), device)?;

        let sigma = beta.sqrt()?;
        let sqrt_one_minus_alpha_bar = alpha_bar.affine(-1.0, 1.0)?.sqrt()?;

        Ok(Self {
            num_steps,
            beta,
            alpha,
            alpha_bar,
            sigma,
            sqrt_one_minus_alpha_bar,
        })
    }

    /// Samples from the model by iteratively denoising random noise.
    ///
    /// # Arguments
    /// * `model` - The trained epsilon-theta model.
    /// * `cond` - Conditional context (encoded history).
    /// * `asset_ids` - Asset ID tensor [batch].
    /// * `shape` - Shape of the output tensor [batch, channels, time].
    pub fn sample(
        &self,
        model: &EpsilonTheta,
        cond: &Tensor,
        asset_ids: &Tensor,
        shape: (usize, usize, usize), // [batch, channels, time]
    ) -> Result<Tensor> {
        let device = cond.device();
        let mut x = Tensor::randn(0.0f32, 1.0f32, shape, device)?;

        // Reverse diffusion process
        for t in (0..self.num_steps).rev() {
            let time_tensor = Tensor::new(&[t as f32], device)?.unsqueeze(0)?; // [1, 1]
            
            // Predict noise
            let epsilon_theta = model.forward(&x, &time_tensor, asset_ids, cond, false)?;

            // Compute mean
            // mu = 1/sqrt(alpha_t) * (x_t - beta_t/sqrt(1-alpha_bar_t) * epsilon)
            
            let alpha_t = self.alpha.get(t)?.broadcast_as(shape)?;
            let beta_t = self.beta.get(t)?;
            let sqrt_one_minus_alpha_bar_t = self.sqrt_one_minus_alpha_bar.get(t)?;
            
            let coeff = (beta_t / sqrt_one_minus_alpha_bar_t)?.broadcast_as(shape)?;
            let mean = ((&x - (epsilon_theta * coeff)?)? / alpha_t.sqrt()?)?;

            if t > 0 {
                let z = Tensor::randn(0.0f32, 1.0f32, shape, device)?;
                let sigma_t = self.sigma.get(t)?.broadcast_as(shape)?;
                x = (mean + (z * sigma_t)?)?;
            } else {
                x = mean;
            }
        }

        Ok(x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::Device;

    #[test]
    fn test_diffusion_schedule() -> Result<()> {
        let device = Device::Cpu;
        let num_steps = 100;
        let diffusion = GaussianDiffusion::new(num_steps, &device)?;

        assert_eq!(diffusion.num_steps, num_steps);
        assert_eq!(diffusion.beta.dims1()?, num_steps);
        assert_eq!(diffusion.alpha.dims1()?, num_steps);
        assert_eq!(diffusion.alpha_bar.dims1()?, num_steps);

        // Check beta range
        let betas = diffusion.beta.to_vec1::<f32>()?;
        assert!((betas[0] - 1e-4).abs() < 1e-6);
        assert!((betas[num_steps - 1] - 0.02).abs() < 1e-6);

        // Check alpha bar monotonicity (should decrease)
        let alpha_bars = diffusion.alpha_bar.to_vec1::<f32>()?;
        for i in 1..num_steps {
            assert!(alpha_bars[i] < alpha_bars[i-1]);
        }

        Ok(())
    }
}

use candle_core::{Tensor, Result, Device};
use crate::models::time_grad::EpsilonTheta;

/// Gaussian Diffusion process for probabilistic time-series forecasting.
/// Implements the forward diffusion (adding noise) and reverse diffusion (denoising) steps.
pub struct GaussianDiffusion {
    pub num_steps: usize,
    #[allow(dead_code)]
    pub beta: Tensor,
    #[allow(dead_code)]
    pub alpha: Tensor,
    pub alpha_bar: Tensor,
    #[allow(dead_code)]
    pub sigma: Tensor,
    #[allow(dead_code)]
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

    /// Samples from the model by iteratively denoising random noise (DDPM — used in training).
    ///
    /// # Arguments
    /// * `model` - The trained epsilon-theta model.
    /// * `cond` - Conditional context (encoded history).
    /// * `asset_ids` - Asset ID tensor [batch].
    /// * `shape` - Shape of the output tensor [batch, channels, time].
    #[allow(dead_code)]
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

    /// DDIM (Denoising Diffusion Implicit Models) fast sampler.
    /// Uses a subset of diffusion steps for much faster inference with minimal quality loss.
    ///
    /// # Arguments
    /// * `model` - The trained epsilon-theta model.
    /// * `cond` - Conditional context [batch, cond_dim, 1].
    /// * `asset_ids` - Asset ID tensor [batch].
    /// * `shape` - Shape of the output tensor [batch, channels, time].
    /// * `ddim_steps` - Number of DDIM steps (e.g., 25 instead of 200).
    /// * `eta` - Stochasticity: 0.0 = deterministic DDIM, 1.0 = DDPM equivalent.
    pub fn sample_ddim(
        &self,
        model: &EpsilonTheta,
        cond: &Tensor,
        asset_ids: &Tensor,
        shape: (usize, usize, usize),
        ddim_steps: usize,
        eta: f64,
    ) -> Result<Tensor> {
        let device = cond.device();
        let mut x = Tensor::randn(0.0f32, 1.0f32, shape, device)?;

        // Create evenly-spaced timestep subsequence
        let step_size = self.num_steps / ddim_steps;
        let timesteps: Vec<usize> = (0..ddim_steps).map(|i| i * step_size).rev().collect();

        let alpha_bar_vec = self.alpha_bar.to_vec1::<f32>()?;

        for (i, &t) in timesteps.iter().enumerate() {
            let time_tensor = Tensor::new(&[t as f32], device)?
                .unsqueeze(0)?
                .broadcast_as((shape.0, 1))?
                .contiguous()?;

            // Predict noise
            let epsilon_pred = model.forward(&x, &time_tensor, asset_ids, cond, false)?;

            let alpha_bar_t = alpha_bar_vec[t] as f64;
            let sqrt_alpha_bar_t = alpha_bar_t.sqrt();
            let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t).sqrt();

            // Predicted x_0 = (x_t - sqrt(1-ᾱ_t) * ε) / sqrt(ᾱ_t)
            let pred_x0 = ((&x - epsilon_pred.affine(sqrt_one_minus_alpha_bar_t, 0.0))?
                .affine(1.0 / sqrt_alpha_bar_t, 0.0))?;

            // Clamp predicted x0 for stability
            let pred_x0 = pred_x0.clamp(-3.0f32, 3.0f32)?;

            if i < timesteps.len() - 1 {
                let t_prev = timesteps[i + 1];
                let alpha_bar_t_prev = alpha_bar_vec[t_prev] as f64;

                // DDIM variance
                let sigma = if eta > 0.0 {
                    let sigma_sq = eta * eta * (1.0 - alpha_bar_t_prev) / (1.0 - alpha_bar_t)
                        * (1.0 - alpha_bar_t / alpha_bar_t_prev);
                    sigma_sq.max(0.0).sqrt()
                } else {
                    0.0
                };

                let sqrt_alpha_bar_prev = alpha_bar_t_prev.sqrt();
                let dir_coeff = ((1.0 - alpha_bar_t_prev - sigma * sigma).max(0.0)).sqrt();

                // x_{t-1} = sqrt(ᾱ_{t-1}) * pred_x0 + dir_coeff * ε_pred + σ * noise
                x = (pred_x0.affine(sqrt_alpha_bar_prev, 0.0)?
                    + epsilon_pred.affine(dir_coeff, 0.0)?)?;

                if sigma > 1e-8 {
                    let noise = Tensor::randn(0.0f32, 1.0f32, shape, device)?;
                    x = (x + noise.affine(sigma, 0.0)?)?;
                }
            } else {
                // Last step: just return predicted x_0
                x = pred_x0;
            }
        }

        Ok(x)
    }

    /// Batched DDIM sampling: runs multiple MC simulations in a single batch.
    /// This is the primary optimization for CPU inference — amortizes overhead.
    ///
    /// # Arguments
    /// * `model` - The trained epsilon-theta model.
    /// * `cond` - Single conditional context [1, cond_dim, 1] — will be broadcast.
    /// * `asset_id` - Single asset ID (u32).
    /// * `batch_size` - Number of parallel MC samples.
    /// * `sample_len` - Number of forecast steps to sample per path in one pass.
    /// * `ddim_steps` - Number of DDIM denoising steps.
    /// * `eta` - DDIM stochasticity parameter.
    pub fn sample_ddim_batched(
        &self,
        model: &EpsilonTheta,
        cond: &Tensor,  // [1, cond_dim, 1]
        asset_id: u32,
        batch_size: usize,
        sample_len: usize,
        ddim_steps: usize,
        eta: f64,
    ) -> Result<Tensor> {
        let device = cond.device();

        // Broadcast conditioning to batch
        let cond_batched = cond.broadcast_as((batch_size, cond.dim(1)?, cond.dim(2)?))?
            .contiguous()?;
        let asset_ids = Tensor::from_vec(vec![asset_id; batch_size], (batch_size,), device)?;

        self.sample_ddim(model, &cond_batched, &asset_ids, (batch_size, 1, sample_len), ddim_steps, eta)
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

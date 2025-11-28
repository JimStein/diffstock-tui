use candle_core::{Tensor, Result, Device};
use crate::models::time_grad::EpsilonTheta;

pub struct GaussianDiffusion {
    pub num_steps: usize,
    pub beta: Tensor,
    pub alpha: Tensor,
    pub _alpha_bar: Tensor,
    pub sigma: Tensor,
    pub _sqrt_alpha_bar: Tensor,
    pub sqrt_one_minus_alpha_bar: Tensor,
}

impl GaussianDiffusion {
    pub fn new(num_steps: usize, device: &Device) -> Result<Self> {
        // Linear beta schedule
        let beta_start = 1e-4;
        let beta_end = 0.1; // Adjusted for fewer steps in TUI
        
        let betas: Vec<f32> = (0..num_steps)
            .map(|i| (beta_start + (beta_end - beta_start) * (i as f64 / (num_steps - 1) as f64)) as f32)
            .collect();
        
        let beta = Tensor::new(betas.as_slice(), device)?;
        let alpha = beta.affine(-1.0, 1.0)?;
        
        let mut alpha_bar_vec = Vec::with_capacity(num_steps);
        let mut cum_prod = 1.0f32;
        for &a in betas.iter() {
            cum_prod *= 1.0f32 - a;
            alpha_bar_vec.push(cum_prod);
        }
        let alpha_bar = Tensor::new(alpha_bar_vec.as_slice(), device)?;

        let sigma = beta.sqrt()?;
        let sqrt_alpha_bar = alpha_bar.sqrt()?;
        let sqrt_one_minus_alpha_bar = alpha_bar.affine(-1.0, 1.0)?.sqrt()?;

        Ok(Self {
            num_steps,
            beta,
            alpha,
            _alpha_bar: alpha_bar,
            sigma,
            _sqrt_alpha_bar: sqrt_alpha_bar,
            sqrt_one_minus_alpha_bar,
        })
    }

    pub fn sample(
        &self,
        model: &EpsilonTheta,
        cond: &Tensor,
        shape: (usize, usize, usize), // [batch, channels, time]
    ) -> Result<Tensor> {
        let device = cond.device();
        let mut x = Tensor::randn(0.0f32, 1.0f32, shape, device)?;

        // Reverse diffusion process
        for t in (0..self.num_steps).rev() {
            let time_tensor = Tensor::new(&[t as f32], device)?.unsqueeze(0)?; // [1, 1]
            
            // Predict noise
            let epsilon_theta = model.forward(&x, &time_tensor, cond)?;

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

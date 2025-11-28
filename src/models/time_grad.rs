use candle_core::{Module, Result, Tensor};
use candle_nn::{Conv1d, Conv1dConfig, Linear, VarBuilder, LSTMConfig, LSTM, RNN};

// --- 1. Diffusion Embedding ---
// Encodes the diffusion step 'k' into a vector.
pub struct DiffusionEmbedding {
    projection1: Linear,
    projection2: Linear,
}

impl DiffusionEmbedding {
    pub fn new(dim: usize, vb: VarBuilder) -> Result<Self> {
        // Input is scalar time step (1), project to dim
        let projection1 = candle_nn::linear(1, dim, vb.pp("projection1"))?;
        let projection2 = candle_nn::linear(dim, dim, vb.pp("projection2"))?;
        Ok(Self { projection1, projection2 })
    }

    pub fn forward(&self, diffusion_steps: &Tensor) -> Result<Tensor> {
        // Sinusoidal embedding logic would go here. 
        // For simplicity in this prototype, we project the raw step.
        // In a real implementation: [sin(pos * w), cos(pos * w), ...]
        
        // Assuming diffusion_steps is [batch_size, 1]
        let x = self.projection1.forward(diffusion_steps)?;
        let x = candle_nn::ops::silu(&x)?;
        let x = self.projection2.forward(&x)?;
        let x = candle_nn::ops::silu(&x)?;
        Ok(x)
    }
}

// --- 2. Residual Block ---
// Dilated convolution block with gated activation.
pub struct ResidualBlock {
    dilated_conv: Conv1d,
    diffusion_projection: Linear,
    conditioner_projection: Conv1d,
    output_projection: Conv1d,
}

impl ResidualBlock {
    pub fn new(
        residual_channels: usize,
        dilation_channels: usize,
        dilation: usize,
        vb: VarBuilder,
    ) -> Result<Self> {
        let conv_cfg = Conv1dConfig {
            padding: dilation, // Causal padding
            dilation,
            ..Default::default()
        };

        let dilated_conv = candle_nn::conv1d(
            residual_channels,
            2 * dilation_channels, // Double for gate + filter
            3, // Kernel size
            conv_cfg,
            vb.pp("dilated_conv"),
        )?;

        let diffusion_projection = candle_nn::linear(
            residual_channels, // Assuming embedding dim == residual channels
            2 * dilation_channels,
            vb.pp("diffusion_projection"),
        )?;

        let conditioner_projection = candle_nn::conv1d(
            1, // Assuming 1D conditioner (hidden state)
            2 * dilation_channels,
            1, // Kernel 1
            Default::default(),
            vb.pp("conditioner_projection"),
        )?;

        let output_projection = candle_nn::conv1d(
            dilation_channels,
            2 * residual_channels, // For residual + skip
            1,
            Default::default(),
            vb.pp("output_projection"),
        )?;

        Ok(Self {
            dilated_conv,
            diffusion_projection,
            conditioner_projection,
            output_projection,
        })
    }

    pub fn forward(&self, x: &Tensor, diffusion_emb: &Tensor, cond: &Tensor) -> Result<(Tensor, Tensor)> {
        // x: [batch, channels, time]
        // diffusion_emb: [batch, channels] 
        // cond: [batch, 1, time]

        // 1. Dilated Conv
        let h = self.dilated_conv.forward(x)?;
        
        // 2. Add Conditioner
        let h_cond = self.conditioner_projection.forward(cond)?;
        let h = (h + h_cond)?;

        // 3. Add Diffusion Embedding
        let diffusion_emb = self.diffusion_projection.forward(diffusion_emb)?;
        let diffusion_emb = diffusion_emb.unsqueeze(2)?; // [batch, 2*dilation_channels, 1]
        let h = (h + diffusion_emb)?;
        
        // 4. Gated Activation
        // Split into filter and gate
        let chunks = h.chunk(2, 1)?;
        let filter = chunks[0].tanh()?;
        let gate = candle_nn::ops::sigmoid(&chunks[1])?;
        let h = filter.mul(&gate)?;

        // 5. Output Projection
        let out = self.output_projection.forward(&h)?;
        let chunks = out.chunk(2, 1)?;
        let residual = &chunks[0];
        let skip = &chunks[1];

        let out_residual = (x + residual)?; // Residual connection
        let out_residual = (out_residual / (2.0f64).sqrt())?;

        Ok((out_residual, skip.clone()))
    }
}

// --- 3. EpsilonTheta (Denoising Network) ---
pub struct EpsilonTheta {
    input_projection: Conv1d,
    diffusion_embedding: DiffusionEmbedding,
    residual_layers: Vec<ResidualBlock>,
    skip_projection: Conv1d,
    output_projection: Conv1d,
}

impl EpsilonTheta {
    pub fn new(
        input_channels: usize,
        residual_channels: usize,
        dilation_channels: usize,
        num_layers: usize,
        vb: VarBuilder,
    ) -> Result<Self> {
        let input_projection = candle_nn::conv1d(
            input_channels,
            residual_channels,
            1,
            Default::default(),
            vb.pp("input_projection"),
        )?;

        let diffusion_embedding = DiffusionEmbedding::new(residual_channels, vb.pp("diffusion_embedding"))?;

        let mut residual_layers = Vec::new();
        for i in 0..num_layers {
            let dilation = 2usize.pow(i as u32);
            residual_layers.push(ResidualBlock::new(
                residual_channels,
                dilation_channels,
                dilation,
                vb.pp(&format!("residual_block_{}", i)),
            )?);
        }

        let skip_projection = candle_nn::conv1d(
            residual_channels,
            residual_channels,
            1,
            Default::default(),
            vb.pp("skip_projection"),
        )?;

        let output_projection = candle_nn::conv1d(
            residual_channels,
            input_channels, // Predict noise (same shape as input)
            1,
            Default::default(),
            vb.pp("output_projection"),
        )?;

        Ok(Self {
            input_projection,
            diffusion_embedding,
            residual_layers,
            skip_projection,
            output_projection,
        })
    }

    pub fn forward(&self, x: &Tensor, time_steps: &Tensor, cond: &Tensor) -> Result<Tensor> {
        let mut x = self.input_projection.forward(x)?;
        let diffusion_emb = self.diffusion_embedding.forward(time_steps)?;
        
        let mut skip_connections = Vec::new();

        for layer in &self.residual_layers {
            let (next_x, skip) = layer.forward(&x, &diffusion_emb, cond)?;
            x = next_x;
            skip_connections.push(skip);
        }

        // Sum skip connections
        let mut total_skip = skip_connections[0].clone();
        for skip in skip_connections.iter().skip(1) {
            total_skip = (total_skip + skip)?;
        }

        let x = (total_skip / (skip_connections.len() as f64).sqrt())?;
        let x = self.skip_projection.forward(&x)?;
        let x = candle_nn::ops::silu(&x)?;
        let x = self.output_projection.forward(&x)?;

        Ok(x)
    }
}

// --- 4. RNN Encoder ---
pub struct RNNEncoder {
    lstm: LSTM,
    projection: Linear,
}

impl RNNEncoder {
    pub fn new(input_dim: usize, hidden_dim: usize, vb: VarBuilder) -> Result<Self> {
        let cfg = LSTMConfig {
            layer_idx: 0,
            ..Default::default()
        };
        let lstm = candle_nn::lstm(input_dim, hidden_dim, cfg, vb.pp("lstm"))?;
        let projection = candle_nn::linear(hidden_dim, 1, vb.pp("projection"))?; // Project to 1D condition
        Ok(Self { lstm, projection })
    }

    pub fn forward(&self, x: &Tensor) -> Result<Tensor> {
        // x: [batch, seq_len, input_dim]
        // We only care about the final hidden state for the next step prediction
        let states = self.lstm.seq(x)?;
        // Take the last state
        let last_state = states.last().ok_or_else(|| candle_core::Error::Msg("Empty LSTM sequence".into()))?;
        let h_t = &last_state.h;
        let cond = self.projection.forward(h_t)?;
        Ok(cond)
    }
}

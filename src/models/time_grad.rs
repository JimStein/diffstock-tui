use candle_core::{Module, Result, Tensor, DType};
use candle_nn::{Conv1d, Conv1dConfig, Linear, VarBuilder, LSTMConfig, LSTM, RNN, Embedding};

// ── Dropout helper ─────────────────────────────────────────────────────────────
/// Inverted dropout: during training, randomly zeros elements with probability `p`
/// and scales remaining elements by 1/(1-p). During inference, returns input unchanged.
pub fn dropout(x: &Tensor, p: f64, train: bool) -> Result<Tensor> {
    if !train || p <= 0.0 || p >= 1.0 {
        return Ok(x.clone());
    }
    let rand_t = Tensor::rand(0.0f32, 1.0f32, x.shape(), x.device())?;
    let threshold = Tensor::full(p as f32, x.shape(), x.device())?;
    // mask: 1.0 where rand >= threshold (keep), 0.0 where rand < threshold (drop)
    let mask = rand_t.ge(&threshold)?.to_dtype(DType::F32)?;
    let scale = 1.0 / (1.0 - p);
    (x.mul(&mask))?.affine(scale, 0.0)
}

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
    dropout_rate: f64,
}

impl ResidualBlock {
    pub fn new(
        residual_channels: usize,
        dilation_channels: usize,
        dilation: usize,
        dropout_rate: f64,
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
            dropout_rate,
        })
    }

    pub fn forward(&self, x: &Tensor, diffusion_emb: &Tensor, cond: &Tensor, train: bool) -> Result<(Tensor, Tensor)> {
        // x: [batch, channels, time]
        // diffusion_emb: [batch, channels] 
        // cond: [batch, 1, time]

        // 1. Dilated Conv
        let h = self.dilated_conv.forward(x)?;
        
        // 2. Add Conditioner
        let h_cond = self.conditioner_projection.forward(cond)?;
        let h = h.broadcast_add(&h_cond)?;

        // 3. Add Diffusion Embedding
        let diffusion_emb = self.diffusion_projection.forward(diffusion_emb)?;
        let diffusion_emb = diffusion_emb.unsqueeze(2)?; // [batch, 2*dilation_channels, 1]
        let h = h.broadcast_add(&diffusion_emb)?;
        
        // 4. Gated Activation
        // Split into filter and gate
        let chunks = h.chunk(2, 1)?;
        let filter = chunks[0].tanh()?;
        let gate = candle_nn::ops::sigmoid(&chunks[1])?;
        let h = filter.mul(&gate)?;

        // Apply dropout after gated activation
        let h = dropout(&h, self.dropout_rate, train)?;

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
    asset_embedding: Embedding,
    residual_layers: Vec<ResidualBlock>,
    skip_projection: Conv1d,
    output_projection: Conv1d,
    dropout_rate: f64,
}

impl EpsilonTheta {
    pub fn new(
        input_channels: usize,
        residual_channels: usize,
        dilation_channels: usize,
        num_layers: usize,
        num_assets: usize,
        dropout_rate: f64,
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
        let asset_embedding = candle_nn::embedding(num_assets, residual_channels, vb.pp("asset_embedding"))?;

        let mut residual_layers = Vec::new();
        for i in 0..num_layers {
            let dilation = 2usize.pow(i as u32);
            residual_layers.push(ResidualBlock::new(
                residual_channels,
                dilation_channels,
                dilation,
                dropout_rate,
                vb.pp(format!("residual_block_{}", i)),
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
            asset_embedding,
            residual_layers,
            skip_projection,
            output_projection,
            dropout_rate,
        })
    }

    pub fn forward(&self, x: &Tensor, time_steps: &Tensor, asset_ids: &Tensor, cond: &Tensor, train: bool) -> Result<Tensor> {
        let mut x = self.input_projection.forward(x)?;
        let diffusion_emb = self.diffusion_embedding.forward(time_steps)?;
        let asset_emb = self.asset_embedding.forward(asset_ids)?;
        
        // Combine embeddings (Add)
        let combined_emb = (diffusion_emb + asset_emb)?;
        
        let mut skip_connections = Vec::new();

        for layer in &self.residual_layers {
            let (next_x, skip) = layer.forward(&x, &combined_emb, cond, train)?;
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
        let x = dropout(&x, self.dropout_rate, train)?;
        let x = self.output_projection.forward(&x)?;

        Ok(x)
    }
}

// --- 4. Multi-layer RNN Encoder ---
pub struct RNNEncoder {
    lstm_layers: Vec<LSTM>,
    projection: Linear,
    dropout_rate: f64,
}

impl RNNEncoder {
    pub fn new(input_dim: usize, hidden_dim: usize, num_layers: usize, dropout_rate: f64, vb: VarBuilder) -> Result<Self> {
        let mut lstm_layers = Vec::with_capacity(num_layers);
        for i in 0..num_layers {
            let in_dim = if i == 0 { input_dim } else { hidden_dim };
            let cfg = LSTMConfig {
                layer_idx: i,
                ..Default::default()
            };
            lstm_layers.push(candle_nn::lstm(in_dim, hidden_dim, cfg, vb.pp(format!("lstm_{}", i)))?);
        }
        let projection = candle_nn::linear(hidden_dim, 1, vb.pp("projection"))?;
        Ok(Self { lstm_layers, projection, dropout_rate })
    }

    pub fn forward(&self, x: &Tensor, train: bool) -> Result<Tensor> {
        // x: [batch, seq_len, input_dim]
        let mut current_input = x.clone();
        let num_layers = self.lstm_layers.len();

        let mut last_h = None;

        for (i, lstm) in self.lstm_layers.iter().enumerate() {
            let states = lstm.seq(&current_input)?;
            last_h = Some(states.last().ok_or_else(|| candle_core::Error::Msg("Empty LSTM sequence".into()))?.h.clone());

            if i < num_layers - 1 {
                // Build hidden state sequence for next layer: [batch, seq_len, hidden_dim]
                let hidden_seq: Vec<Tensor> = states.iter().map(|s| s.h.clone()).collect();
                current_input = Tensor::stack(&hidden_seq, 1)?;
                // Dropout between LSTM layers
                current_input = dropout(&current_input, self.dropout_rate, train)?;
            }
        }

        let h_t = last_h.ok_or_else(|| candle_core::Error::Msg("No LSTM layers".into()))?;
        let cond = self.projection.forward(&h_t)?;
        Ok(cond)
    }
}

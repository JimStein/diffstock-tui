#!/usr/bin/env python3
import argparse
import math
import pathlib
import sys


def _import_deps():
    try:
        import torch
        import torch.nn as nn
        from safetensors import safe_open
    except ImportError as exc:
        print(f"[export_to_onnx] missing dependency: {exc}", file=sys.stderr)
        print(
            "[export_to_onnx] install dependencies with: pip install torch safetensors onnx",
            file=sys.stderr,
        )
        return None
    return torch, nn, safe_open


def build_models(torch, nn, input_dim, hidden_dim, lstm_layers, num_layers, num_assets):
    class DiffusionEmbedding(nn.Module):
        def __init__(self):
            super().__init__()
            self.projection1 = nn.Linear(1, hidden_dim)
            self.projection2 = nn.Linear(hidden_dim, hidden_dim)

        def forward(self, diffusion_steps):
            x = torch.nn.functional.silu(self.projection1(diffusion_steps))
            x = torch.nn.functional.silu(self.projection2(x))
            return x

    class ResidualBlock(nn.Module):
        def __init__(self, dilation):
            super().__init__()
            self.dilated_conv = nn.Conv1d(
                hidden_dim,
                2 * hidden_dim,
                kernel_size=3,
                padding=dilation,
                dilation=dilation,
            )
            self.diffusion_projection = nn.Linear(hidden_dim, 2 * hidden_dim)
            self.conditioner_projection = nn.Conv1d(1, 2 * hidden_dim, kernel_size=1)
            self.output_projection = nn.Conv1d(hidden_dim, 2 * hidden_dim, kernel_size=1)

        def forward(self, x, diffusion_emb, cond):
            h = self.dilated_conv(x)
            h = h + self.conditioner_projection(cond)
            h = h + self.diffusion_projection(diffusion_emb).unsqueeze(2)
            filter_part, gate_part = torch.chunk(h, 2, dim=1)
            h = torch.tanh(filter_part) * torch.sigmoid(gate_part)
            out = self.output_projection(h)
            residual, skip = torch.chunk(out, 2, dim=1)
            out_residual = (x + residual) / math.sqrt(2.0)
            return out_residual, skip

    class EpsilonTheta(nn.Module):
        def __init__(self):
            super().__init__()
            self.input_projection = nn.Conv1d(1, hidden_dim, kernel_size=1)
            self.diffusion_embedding = DiffusionEmbedding()
            self.asset_embedding = nn.Embedding(num_assets, hidden_dim)
            for i in range(num_layers):
                setattr(self, f"residual_block_{i}", ResidualBlock(dilation=2**i))
            self.skip_projection = nn.Conv1d(hidden_dim, hidden_dim, kernel_size=1)
            self.output_projection = nn.Conv1d(hidden_dim, 1, kernel_size=1)

        def forward(self, x, time_steps, asset_ids, cond):
            x = self.input_projection(x)
            diffusion_emb = self.diffusion_embedding(time_steps)
            asset_emb = self.asset_embedding(asset_ids)
            combined_emb = diffusion_emb + asset_emb

            skips = []
            for i in range(num_layers):
                block = getattr(self, f"residual_block_{i}")
                x, skip = block(x, combined_emb, cond)
                skips.append(skip)

            total_skip = skips[0]
            for s in skips[1:]:
                total_skip = total_skip + s

            x = total_skip / math.sqrt(float(len(skips)))
            x = torch.nn.functional.silu(self.skip_projection(x))
            return self.output_projection(x)

    class RNNEncoder(nn.Module):
        def __init__(self):
            super().__init__()
            self.lstm_0 = nn.LSTM(input_dim, hidden_dim, batch_first=True)
            self.lstm_1 = nn.LSTM(hidden_dim, hidden_dim, batch_first=True)
            self.projection = nn.Linear(hidden_dim, 1)

        def forward(self, x):
            out, _ = self.lstm_0(x)
            out, (h_last, _) = self.lstm_1(out)
            h = h_last[-1]
            return self.projection(h)

    if lstm_layers != 2:
        raise ValueError("Current exporter supports lstm_layers=2 only.")

    return RNNEncoder(), EpsilonTheta()


def _load_state_dict_from_safetensors(torch, safe_open, module, prefix, path):
    expected = module.state_dict()
    loaded = {}
    missing = []
    with safe_open(str(path), framework="np") as sf:
        keys = set(sf.keys())
        for name, tensor in expected.items():
            full_name = f"{prefix}.{name}"
            candidate_names = [full_name]
            if full_name.startswith("encoder.lstm_1.") and full_name.endswith("_l0"):
                candidate_names.append(full_name[:-3] + "_l1")

            chosen_name = None
            for candidate in candidate_names:
                if candidate in keys:
                    chosen_name = candidate
                    break

            if chosen_name is None:
                missing.append(full_name)
                continue

            arr = sf.get_tensor(chosen_name)
            val = torch.from_numpy(arr)
            if tuple(val.shape) != tuple(tensor.shape):
                raise ValueError(
                    f"shape mismatch for {chosen_name}: expected {tuple(tensor.shape)} got {tuple(val.shape)}"
                )
            loaded[name] = val

    if missing:
        preview = ", ".join(missing[:8])
        raise ValueError(f"missing safetensors keys (first): {preview}")

    module.load_state_dict(loaded, strict=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export executable TimeGrad ONNX from safetensors.")
    parser.add_argument("--input", required=True, help="Path to model_weights.safetensors")
    parser.add_argument("--output", required=True, help="Path to denoiser ONNX output (e.g., model_weights.onnx)")
    parser.add_argument("--encoder-output", default=None, help="Optional encoder ONNX output path")
    parser.add_argument("--opset", type=int, default=17)
    parser.add_argument("--input-dim", type=int, default=2)
    parser.add_argument("--hidden-dim", type=int, default=512)
    parser.add_argument("--lstm-layers", type=int, default=2)
    parser.add_argument("--num-layers", type=int, default=8)
    parser.add_argument("--num-assets", type=int, default=30)
    parser.add_argument("--lookback", type=int, default=60)
    parser.add_argument("--forecast", type=int, default=10)
    args = parser.parse_args()

    dep_bundle = _import_deps()
    if dep_bundle is None:
        return 4
    torch, nn, safe_open = dep_bundle

    input_path = pathlib.Path(args.input)
    denoiser_out = pathlib.Path(args.output)
    encoder_out = pathlib.Path(args.encoder_output) if args.encoder_output else denoiser_out.with_suffix(".encoder.onnx")

    if not input_path.exists():
        print(f"[export_to_onnx] input not found: {input_path}", file=sys.stderr)
        return 2

    denoiser_out.parent.mkdir(parents=True, exist_ok=True)
    encoder_out.parent.mkdir(parents=True, exist_ok=True)

    encoder, denoiser = build_models(
        torch,
        nn,
        input_dim=args.input_dim,
        hidden_dim=args.hidden_dim,
        lstm_layers=args.lstm_layers,
        num_layers=args.num_layers,
        num_assets=args.num_assets,
    )
    encoder.eval()
    denoiser.eval()

    try:
        _load_state_dict_from_safetensors(torch, safe_open, encoder, "encoder", input_path)
        _load_state_dict_from_safetensors(torch, safe_open, denoiser, "model", input_path)
    except Exception as exc:
        print(f"[export_to_onnx] failed to map weights: {exc}", file=sys.stderr)
        return 6

    with torch.no_grad():
        x_hist = torch.randn(1, args.lookback, args.input_dim, dtype=torch.float32)
        torch.onnx.export(
            encoder,
            (x_hist,),
            str(encoder_out),
            input_names=["x_hist"],
            output_names=["cond"],
            dynamic_axes={"x_hist": {0: "batch"}, "cond": {0: "batch"}},
            opset_version=args.opset,
        )

        x_t = torch.randn(1, 1, args.forecast, dtype=torch.float32)
        time_steps = torch.zeros(1, 1, dtype=torch.float32)
        asset_ids = torch.zeros(1, dtype=torch.long)
        cond = torch.randn(1, 1, 1, dtype=torch.float32)
        torch.onnx.export(
            denoiser,
            (x_t, time_steps, asset_ids, cond),
            str(denoiser_out),
            input_names=["x_t", "time_steps", "asset_ids", "cond"],
            output_names=["epsilon_pred"],
            dynamic_axes={
                "x_t": {0: "batch", 2: "time"},
                "time_steps": {0: "batch"},
                "asset_ids": {0: "batch"},
                "cond": {0: "batch"},
                "epsilon_pred": {0: "batch", 2: "time"},
            },
            opset_version=args.opset,
        )

    print(f"[export_to_onnx] exported denoiser ONNX: {denoiser_out}", file=sys.stderr)
    print(f"[export_to_onnx] exported encoder ONNX:  {encoder_out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

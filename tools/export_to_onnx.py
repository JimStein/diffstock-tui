#!/usr/bin/env python3
import argparse
import pathlib
import sys


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export DiffStock model from safetensors to ONNX."
    )
    parser.add_argument("--input", required=True, help="Path to model_weights.safetensors")
    parser.add_argument("--output", required=True, help="Path to output ONNX model")
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)

    if not input_path.exists():
        print(f"[export_to_onnx] input not found: {input_path}", file=sys.stderr)
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        "[export_to_onnx] ONNX conversion pipeline is not yet implemented in this script. "
        "Please replace tools/export_to_onnx.py with your model-specific exporter.",
        file=sys.stderr,
    )
    print(
        "[export_to_onnx] expected invocation: --input model_weights.safetensors --output model_weights.onnx",
        file=sys.stderr,
    )
    return 3


if __name__ == "__main__":
    sys.exit(main())

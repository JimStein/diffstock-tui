#!/usr/bin/env python3
import argparse
import pathlib
import sys


def _load_conversion_deps(project_root: pathlib.Path):
    try:
        import onnx
    except ImportError as exc:
        print(f"[convert_onnx_to_fp16] missing dependency: {exc}", file=sys.stderr)
        print(
            "[convert_onnx_to_fp16] install dependencies with: pip install onnx onnxruntime",
            file=sys.stderr,
        )
        return None

    try:
        from onnxruntime.transformers.float16 import convert_float_to_float16
        return onnx, convert_float_to_float16
    except ImportError:
        runtime_root = project_root / ".runtime" / "ort_dml_1_24_1"
        if runtime_root.exists():
            sys.path.insert(0, str(runtime_root))
            from onnxruntime.transformers.float16 import convert_float_to_float16

            return onnx, convert_float_to_float16

        print(
            "[convert_onnx_to_fp16] missing dependency: onnxruntime.transformers.float16",
            file=sys.stderr,
        )
        print(
            "[convert_onnx_to_fp16] install dependencies with: pip install onnxruntime",
            file=sys.stderr,
        )
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert an ONNX model to DirectML-friendly FP16 without touching training artifacts."
    )
    parser.add_argument("--input", required=True, help="Path to FP32 ONNX input model")
    parser.add_argument("--output", required=True, help="Path to FP16 ONNX output model")
    parser.add_argument(
        "--keep-io-fp32",
        action="store_true",
        help="Keep graph inputs/outputs as FP32 and only convert the internal graph to FP16",
    )
    parser.add_argument(
        "--force-fp16-initializers",
        action="store_true",
        help="Force all float initializers to FP16 instead of only the safe subset",
    )
    args = parser.parse_args()

    project_root = pathlib.Path(__file__).resolve().parents[1]
    dep_bundle = _load_conversion_deps(project_root)
    if dep_bundle is None:
        return 4
    onnx, convert_float_to_float16 = dep_bundle

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    if not input_path.exists():
        print(f"[convert_onnx_to_fp16] input not found: {input_path}", file=sys.stderr)
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)

    model = onnx.load(str(input_path))
    converted = convert_float_to_float16(
        model,
        keep_io_types=args.keep_io_fp32,
        force_fp16_initializers=args.force_fp16_initializers,
    )
    onnx.save(converted, str(output_path))

    io_mode = "fp32-io" if args.keep_io_fp32 else "fp16-io"
    print(
        f"[convert_onnx_to_fp16] wrote {io_mode} DirectML model: {output_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
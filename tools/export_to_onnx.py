#!/usr/bin/env python3
import argparse
import pathlib
import sys


def _import_deps():
    try:
        import onnx  # type: ignore
        from onnx import helper, numpy_helper  # type: ignore
        from safetensors import safe_open  # type: ignore
    except ImportError as exc:
        print(f"[export_to_onnx] missing dependency: {exc}", file=sys.stderr)
        print(
            "[export_to_onnx] install dependencies with: pip install onnx safetensors numpy",
            file=sys.stderr,
        )
        return None
    return onnx, helper, numpy_helper, safe_open


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export DiffStock weights from safetensors to a valid ONNX file. "
            "This emits a weight-archive ONNX graph with tensor constants as outputs."
        )
    )
    parser.add_argument("--input", required=True, help="Path to model_weights.safetensors")
    parser.add_argument("--output", required=True, help="Path to output ONNX model")
    parser.add_argument(
        "--opset",
        type=int,
        default=17,
        help="ONNX opset version to encode (default: 17)",
    )
    args = parser.parse_args()

    dep_bundle = _import_deps()
    if dep_bundle is None:
        return 4
    onnx, helper, numpy_helper, safe_open = dep_bundle

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)

    if not input_path.exists():
        print(f"[export_to_onnx] input not found: {input_path}", file=sys.stderr)
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)

    tensor_names = []
    constant_nodes = []
    graph_outputs = []

    with safe_open(str(input_path), framework="np") as sf:
        tensor_names = list(sf.keys())
        if not tensor_names:
            print("[export_to_onnx] no tensors found in safetensors file", file=sys.stderr)
            return 5

        for idx, tensor_name in enumerate(tensor_names):
            arr = sf.get_tensor(tensor_name)
            out_name = tensor_name
            const_name = f"const_{idx}"
            tensor_proto = numpy_helper.from_array(arr, name=f"value_{idx}")
            constant_nodes.append(
                helper.make_node(
                    "Constant",
                    inputs=[],
                    outputs=[out_name],
                    name=const_name,
                    value=tensor_proto,
                )
            )
            graph_outputs.append(
                helper.make_tensor_value_info(out_name, tensor_proto.data_type, arr.shape)
            )

    graph = helper.make_graph(
        nodes=constant_nodes,
        name="DiffStockWeightsArchive",
        inputs=[],
        outputs=graph_outputs,
        initializer=[],
    )

    model = helper.make_model(
        graph,
        producer_name="diffstock-exporter",
        producer_version="1.0",
        opset_imports=[helper.make_operatorsetid("", args.opset)],
    )
    model.doc_string = (
        "Weight-archive ONNX exported from DiffStock safetensors. "
        "This artifact stores parameter tensors and is suitable for runtime probing/loading checks."
    )
    onnx.checker.check_model(model)
    onnx.save(model, str(output_path))

    print(
        f"[export_to_onnx] exported {len(tensor_names)} tensors from {input_path} -> {output_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

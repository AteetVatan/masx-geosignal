"""Export DistilBART to ONNX for faster CPU inference.

Usage:
    python scripts/export_onnx.py

This creates an ONNX model directory at models/distilbart-cnn-onnx/
that is ~2× faster than the vanilla PyTorch model on CPU.

Only needs to be run once.  The local_summarizer will auto-detect
the ONNX model and use it if available.

# Option 1: Just re-run (it should resume from cache)
python scripts/export_onnx.py
# Option 2: Set HF_HUB_ENABLE_HF_TRANSFER=1 for faster downloads (if hf_transfer is installed)
pip install hf_transfer
$env:HF_HUB_ENABLE_HF_TRANSFER="1"
python scripts/export_onnx.py


"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import get_settings


def main() -> None:
    settings = get_settings()
    model_name = settings.local_summarizer_model
    output_dir = Path(settings.local_summarizer_onnx_dir)

    print(f"Exporting {model_name} to ONNX...")
    print(f"Output directory: {output_dir}")

    from optimum.onnxruntime import ORTModelForSeq2SeqLM

    # Export: downloads model if needed, converts to ONNX
    ort_model = ORTModelForSeq2SeqLM.from_pretrained(
        model_name,
        export=True,
    )
    ort_model.save_pretrained(str(output_dir))

    # Also save the tokenizer alongside
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    tokenizer.save_pretrained(str(output_dir))

    print(f"\n✓ ONNX model exported to {output_dir}/")
    print(f"  Files: {[f.name for f in output_dir.iterdir()]}")
    print("  The local_summarizer will auto-detect and use this model.")


if __name__ == "__main__":
    main()

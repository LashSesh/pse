#!/bin/bash
set -e

echo "Building PSE for WebAssembly..."
cd "$(dirname "$0")/.."

# Build WASM
wasm-pack build crates/pse-wasm --target web --out-dir ../../web/pkg

echo ""
echo "Build complete!"
echo "WASM size: $(wc -c < web/pkg/pse_wasm_bg.wasm) bytes"
echo ""
echo "To serve locally:"
echo "  cd web && python3 -m http.server 8080"
echo "  → http://localhost:8080"

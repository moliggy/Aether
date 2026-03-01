#!/bin/bash
# 本地构建 aether-hub 多架构二进制
#
# 前置条件:
#   cargo install cross --git https://github.com/cross-rs/cross
#   Docker 或 Podman 运行中（cross 需要容器环境）
#
# 用法:
#   ./build.sh                    # 构建 linux/amd64 + linux/arm64
#   ./build.sh amd64              # 仅构建 linux/amd64
#   ./build.sh arm64              # 仅构建 linux/arm64
#   ./build.sh --upload v0.1.0    # 构建并上传到 GitHub Release（需要 gh CLI）

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"

# 解析参数
UPLOAD=false
TAG=""
BUILD_TARGETS=""

while [ $# -gt 0 ]; do
    case "$1" in
        --upload)
            UPLOAD=true
            TAG="$2"
            shift 2
            ;;
        amd64|arm64)
            BUILD_TARGETS="$BUILD_TARGETS $1"
            shift
            ;;
        *)
            echo "用法: $0 [amd64|arm64] [--upload <tag>]"
            exit 1
            ;;
    esac
done

# 默认构建所有平台
if [ -z "$BUILD_TARGETS" ]; then
    BUILD_TARGETS="amd64 arm64"
fi

# 检查 cross 是否安装
if ! command -v cross &> /dev/null; then
    echo "❌ 需要安装 cross: cargo install cross --git https://github.com/cross-rs/cross"
    exit 1
fi

# 创建输出目录
mkdir -p "$DIST_DIR"

echo "🔨 开始构建 aether-hub..."
echo "   目标平台: $BUILD_TARGETS"
echo ""

ARTIFACTS=""

for arch in $BUILD_TARGETS; do
    # 映射架构到 Rust target
    case "$arch" in
        amd64) target="x86_64-unknown-linux-gnu" ;;
        arm64) target="aarch64-unknown-linux-gnu" ;;
        *)     echo "❌ 未知架构: $arch"; exit 1 ;;
    esac

    echo ">>> 构建 $arch ($target)..."

    cd "$SCRIPT_DIR"
    cross build --release --target "$target" --locked

    # 提取二进制
    BIN="target/$target/release/aether-hub"
    if [ ! -f "$BIN" ]; then
        echo "❌ 未找到二进制文件: $BIN"
        exit 1
    fi

    # 打包
    ARCHIVE="$DIST_DIR/aether-hub-linux-$arch.tar.gz"
    tar czf "$ARCHIVE" -C "target/$target/release" aether-hub
    ARTIFACTS="$ARTIFACTS $ARCHIVE"

    SIZE=$(du -h "$ARCHIVE" | cut -f1)
    echo "✅ $arch 构建完成: $ARCHIVE ($SIZE)"
    echo ""
done

# 生成校验和
cd "$DIST_DIR"
shasum -a 256 aether-hub-*.tar.gz > SHA256SUMS.txt
echo "📋 SHA256 校验和:"
cat SHA256SUMS.txt
echo ""

# 上传到 GitHub Release
if [ "$UPLOAD" = true ]; then
    if [ -z "$TAG" ]; then
        echo "❌ --upload 需要指定 tag，例如: ./build.sh --upload hub-v0.1.0"
        exit 1
    fi

    if ! command -v gh &> /dev/null; then
        echo "❌ 需要安装 GitHub CLI: brew install gh"
        exit 1
    fi

    echo "📦 上传到 GitHub Release: $TAG"
    cd "$PROJECT_DIR"

    # 创建 tag（如果不存在）
    if ! git rev-parse "$TAG" > /dev/null 2>&1; then
        git tag "$TAG"
        git push origin "$TAG"
    fi

    # 创建 Release 并上传
    gh release create "$TAG" \
        --title "aether-hub ${TAG#hub-}" \
        --generate-notes \
        $ARTIFACTS \
        "$DIST_DIR/SHA256SUMS.txt"

    echo "✅ 上传完成!"
fi

echo "🎉 全部完成!"

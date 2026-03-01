#!/bin/bash
# 智能部署脚本 - 自动检测依赖/代码/迁移变化
#
# 用法:
#   部署/更新:    ./deploy.sh  (自动检测所有变化)
#   强制重建:     ./deploy.sh --rebuild-base
#   更新 Hub:    ./deploy.sh --update-hub
#   强制全部重建: ./deploy.sh --force

set -e
cd "$(dirname "$0")"

# 兼容 docker-compose 和 docker compose
if command -v docker-compose &> /dev/null; then
    DC="docker-compose -f docker-compose.build.yml"
    USE_LEGACY_COMPOSE=true
else
    DC="docker compose -f docker-compose.build.yml"
    USE_LEGACY_COMPOSE=false
fi

compose_up() {
    if [ "$USE_LEGACY_COMPOSE" = true ]; then
        $DC up -d --no-build "$@"
    else
        $DC up -d --no-build --pull never "$@"
    fi
}

# 缓存文件
HASH_FILE=".deps-hash"
CODE_HASH_FILE=".code-hash"
MIGRATION_HASH_FILE=".migration-hash"

# Hub 二进制配置
GITHUB_REPO="fawney19/Aether"
HUB_DIST_DIR="aether-hub/dist"
HUB_VERSION_FILE="$HUB_DIST_DIR/.version"

# 提取 pyproject.toml 中"会影响运行时依赖安装"的最小指纹（与 CI 保持一致）：
# - [build-system] requires / build-backend
# - [project] requires-python / dependencies
# 使用 Python tomllib 解析，不受 TOML 格式变化影响。
pyproject_deps_fingerprint() {
    python3 - <<'PY'
import json, pathlib, tomllib

data = tomllib.loads(pathlib.Path("pyproject.toml").read_text("utf-8"))
project = data.get("project") or {}
build = data.get("build-system") or {}

fingerprint = {
    "requires-python": project.get("requires-python"),
    "dependencies": sorted(project.get("dependencies") or []),
    "build-backend": build.get("build-backend"),
    "build-requires": sorted(build.get("requires") or []),
}

print(json.dumps(fingerprint, sort_keys=True, separators=(",", ":")))
PY
}

# 计算依赖文件的哈希值（包含 Dockerfile.base.local）
calc_deps_hash() {
    {
        cat Dockerfile.base.local 2>/dev/null
        pyproject_deps_fingerprint
        # 前端依赖以 lock 为准（避免仅改 scripts/version 触发 base 重建）
        cat frontend/package-lock.json 2>/dev/null
    } | md5sum | cut -d' ' -f1
}

# 计算代码文件的哈希值（包含 Dockerfile.app.local）
calc_code_hash() {
    {
        cat Dockerfile.app.local 2>/dev/null
        find src -type f -name "*.py" 2>/dev/null | sort | xargs cat 2>/dev/null
        find frontend/src -type f \( -name "*.vue" -o -name "*.ts" -o -name "*.tsx" -o -name "*.js" \) 2>/dev/null | sort | xargs cat 2>/dev/null
    } | md5sum | cut -d' ' -f1
}

# 检测目标平台架构
detect_arch() {
    local machine
    machine=$(uname -m)
    case "$machine" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        *) echo "❌ 不支持的架构: $machine" >&2; exit 1 ;;
    esac
}

# 获取最新 hub release tag
get_latest_hub_tag() {
    curl -sL "https://api.github.com/repos/$GITHUB_REPO/releases" | \
        python3 -c "
import json, sys
releases = json.load(sys.stdin)
for r in releases:
    tag = r.get('tag_name', '')
    if tag.startswith('hub-v') and not r.get('draft') and not r.get('prerelease'):
        print(tag)
        break
" 2>/dev/null
}

# 下载 Hub 预编译二进制
download_hub() {
    local tag="$1"
    local arch
    arch=$(detect_arch)

    if [ -z "$tag" ]; then
        echo ">>> 正在查询最新 Hub 版本..."
        tag=$(get_latest_hub_tag)
        if [ -z "$tag" ]; then
            echo "❌ 无法获取最新 Hub Release，请检查网络或手动指定版本"
            exit 1
        fi
    fi

    echo ">>> Hub 版本: $tag, 架构: $arch"

    # 检查是否已下载
    if [ -f "$HUB_VERSION_FILE" ] && [ "$(cat "$HUB_VERSION_FILE")" = "$tag-$arch" ] && [ -f "$HUB_DIST_DIR/aether-hub" ]; then
        echo ">>> Hub 二进制已是最新 ($tag), 跳过下载."
        return 0
    fi

    mkdir -p "$HUB_DIST_DIR"

    local archive="aether-hub-linux-$arch.tar.gz"
    local url="https://github.com/$GITHUB_REPO/releases/download/$tag/$archive"

    echo ">>> 下载 Hub 二进制: $url"
    curl -L --fail -o "$HUB_DIST_DIR/$archive" "$url" || {
        echo "❌ 下载失败: $url"
        exit 1
    }

    # 解压
    tar xzf "$HUB_DIST_DIR/$archive" -C "$HUB_DIST_DIR"
    chmod +x "$HUB_DIST_DIR/aether-hub"
    rm -f "$HUB_DIST_DIR/$archive"

    # 记录版本
    echo "$tag-$arch" > "$HUB_VERSION_FILE"
    echo "✅ Hub 二进制下载完成."
}

# 确保 Hub 二进制存在
ensure_hub_binary() {
    if [ ! -f "$HUB_DIST_DIR/aether-hub" ]; then
        echo ">>> Hub 二进制不存在，正在下载..."
        download_hub
        return 0
    fi
    return 1
}

# 计算迁移文件的哈希值
calc_migration_hash() {
    find alembic/versions -name "*.py" -type f 2>/dev/null | sort | xargs cat 2>/dev/null | md5sum | cut -d' ' -f1
}

# 检查依赖是否变化
check_deps_changed() {
    local current_hash=$(calc_deps_hash)
    if [ -f "$HASH_FILE" ]; then
        local saved_hash=$(cat "$HASH_FILE")
        if [ "$current_hash" = "$saved_hash" ]; then
            return 1
        fi
    fi
    return 0
}

# 检查代码是否变化
check_code_changed() {
    local current_hash=$(calc_code_hash)
    if [ -f "$CODE_HASH_FILE" ]; then
        local saved_hash=$(cat "$CODE_HASH_FILE")
        if [ "$current_hash" = "$saved_hash" ]; then
            return 1
        fi
    fi
    return 0
}



# 检查迁移是否变化
check_migration_changed() {
    local current_hash=$(calc_migration_hash)
    if [ -f "$MIGRATION_HASH_FILE" ]; then
        local saved_hash=$(cat "$MIGRATION_HASH_FILE")
        if [ "$current_hash" = "$saved_hash" ]; then
            return 1
        fi
    fi
    return 0
}

# 保存哈希
save_deps_hash() { calc_deps_hash > "$HASH_FILE"; }
save_code_hash() { calc_code_hash > "$CODE_HASH_FILE"; }
save_migration_hash() { calc_migration_hash > "$MIGRATION_HASH_FILE"; }

# 构建基础镜像
build_base() {
    echo ">>> Building base image (dependencies)..."
    docker build --pull=false -f Dockerfile.base.local -t aether-base:latest .
    save_deps_hash
}


# 生成版本文件
generate_version_file() {
    # 从 git 获取版本号
    local version
    version=$(git describe --tags --always 2>/dev/null | sed 's/^v//')
    if [ -z "$version" ]; then
        version="unknown"
    fi
    echo ">>> Generating version file: $version"
    cat > src/_version.py << EOF
# Auto-generated by deploy.sh - do not edit
__version__ = '$version'
__version_tuple__ = tuple(int(x) for x in '$version'.split('-')[0].split('.') if x.isdigit())
version = __version__
version_tuple = __version_tuple__
EOF
}

# 构建应用镜像
build_app() {
    echo ">>> Building app image (code only)..."
    generate_version_file
    docker build --pull=false -f Dockerfile.app.local -t aether-app:latest .
    save_code_hash
}

# 运行数据库迁移
run_migration() {
    echo ">>> Running database migration..."

    # 尝试运行 upgrade head，捕获错误
    UPGRADE_OUTPUT=$($DC exec -T app alembic upgrade head 2>&1) && {
        echo "$UPGRADE_OUTPUT"
        save_migration_hash
        return 0
    }

    # 检查是否是因为找不到旧版本（基线重置场景）
    if echo "$UPGRADE_OUTPUT" | grep -q "Can't locate revision"; then
        echo ">>> Detected baseline reset: old revision not found in migrations"
        echo ">>> Clearing old version and stamping to new baseline..."

        # 先清除旧的版本记录，再 stamp 到新基线
        $DC exec -T app python -c "
from sqlalchemy import create_engine, text
import os
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    conn.execute(text('DELETE FROM alembic_version'))
    conn.commit()
print('Old version cleared')
"
        # 获取最新的迁移版本（匹配 revision_id (head) 格式）
        LATEST_VERSION=$($DC exec -T app alembic heads 2>/dev/null | grep -oE '^[0-9a-zA-Z_]+' | head -1)
        if [ -n "$LATEST_VERSION" ]; then
            $DC exec -T app alembic stamp "$LATEST_VERSION"
            echo ">>> Database stamped to $LATEST_VERSION"
            save_migration_hash
        else
            echo ">>> ERROR: Could not determine latest migration version"
            exit 1
        fi
    else
        # 其他错误，直接输出并退出
        echo "$UPGRADE_OUTPUT"
        exit 1
    fi
}

# 强制全部重建
if [ "$1" = "--force" ] || [ "$1" = "-f" ]; then
    echo ">>> Force rebuilding everything..."
    download_hub
    build_base
    build_app
    compose_up --force-recreate
    sleep 3
    run_migration
    docker image prune -f
    echo ">>> Done!"
    $DC ps
    exit 0
fi

# 强制重建基础镜像
if [ "$1" = "--rebuild-base" ] || [ "$1" = "-r" ]; then
    build_base
    echo ">>> Base image rebuilt. Run ./deploy.sh to deploy."
    exit 0
fi

# 更新 Hub 二进制
if [ "$1" = "--update-hub" ]; then
    rm -f "$HUB_VERSION_FILE"
    download_hub
    echo ">>> Hub binary updated. Run ./deploy.sh to deploy."
    exit 0
fi

# 拉取最新代码
echo ">>> Pulling latest code..."
git pull

# 标记是否需要重启
NEED_RESTART=false
BASE_REBUILT=false
HUB_UPDATED=false

# 检查基础镜像是否存在，或依赖是否变化
if ! docker image inspect aether-base:latest >/dev/null 2>&1; then
    echo ">>> Base image not found, building..."
    build_base
    BASE_REBUILT=true
    NEED_RESTART=true
elif check_deps_changed; then
    echo ">>> Dependencies changed, rebuilding base image..."
    build_base
    BASE_REBUILT=true
    NEED_RESTART=true
else
    echo ">>> Dependencies unchanged."
fi

# 确保 Hub 二进制存在
if ensure_hub_binary; then
    HUB_UPDATED=true
    NEED_RESTART=true
else
    echo ">>> Hub binary present."
fi

# 检查代码或迁移是否变化，或者 base 重建了（app 依赖 base）
# 注意：迁移文件打包在镜像中，所以迁移变化也需要重建 app 镜像
MIGRATION_CHANGED=false
if check_migration_changed; then
    MIGRATION_CHANGED=true
fi

if ! docker image inspect aether-app:latest >/dev/null 2>&1; then
    echo ">>> App image not found, building..."
    build_app
    NEED_RESTART=true
elif [ "$BASE_REBUILT" = true ]; then
    echo ">>> Base image rebuilt, rebuilding app image..."
    build_app
    NEED_RESTART=true
elif [ "$HUB_UPDATED" = true ]; then
    echo ">>> Hub binary updated, rebuilding app image..."
    build_app
    NEED_RESTART=true
elif check_code_changed; then
    echo ">>> Code changed, rebuilding app image..."
    build_app
    NEED_RESTART=true
elif [ "$MIGRATION_CHANGED" = true ]; then
    echo ">>> Migration files changed, rebuilding app image..."
    build_app
    NEED_RESTART=true
else
    echo ">>> Code unchanged."
fi

# 检查容器是否在运行
CONTAINERS_RUNNING=true
if [ -z "$($DC ps -q 2>/dev/null)" ]; then
    CONTAINERS_RUNNING=false
fi

# 有变化时重启，或容器未运行时启动
if [ "$NEED_RESTART" = true ]; then
    echo ">>> Restarting services..."
    compose_up
elif [ "$CONTAINERS_RUNNING" = false ]; then
    echo ">>> Containers not running, starting services..."
    compose_up
else
    echo ">>> No changes detected, skipping restart."
fi

# 检查迁移变化（如果前面已经检测到变化并重建了镜像，这里直接运行迁移）
if [ "$MIGRATION_CHANGED" = true ]; then
    echo ">>> Running database migration..."
    sleep 3
    run_migration
else
    echo ">>> Migration unchanged."
fi

# 清理
docker image prune -f >/dev/null 2>&1 || true

echo ">>> Done!"
$DC ps

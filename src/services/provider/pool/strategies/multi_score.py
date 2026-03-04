"""Multi-dimension pool scoring strategy."""

from __future__ import annotations

from typing import Any

import src.services.provider.pool.dimensions  # noqa: F401
from src.services.provider.pool.dimensions import get_preset_dimension, get_preset_names
from src.services.provider.pool.dimensions._helpers import rank_ascending, safe_float
from src.services.provider.pool.strategy import register_pool_strategy

# When LRU is enabled alongside presets, this fraction of the final score
# comes from the LRU rank (tiebreaker to avoid same-score collisions).
_LRU_BLEND_FACTOR = 0.04
# Positional weight decay factor: weight = 1 / (1 + DECAY * index).
_POSITIONAL_DECAY = 0.6


def _normalize_presets_from_config(config: Any) -> tuple[tuple[str, str | None], ...]:
    """Extract enabled (preset_name, mode) tuples from config.scheduling_presets.

    Supports both new SchedulingPreset objects and legacy string lists.
    Excludes ``lru`` since LRU is handled separately as a blend factor.
    """

    raw = getattr(config, "scheduling_presets", ())
    if not isinstance(raw, (list, tuple)):
        return ()

    allowed = get_preset_names() | {"lru"}
    ordered: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    for item in raw:
        preset_name: str | None = None
        enabled = True
        mode: str | None = None

        if hasattr(item, "preset"):
            preset_name = str(getattr(item, "preset", "")).strip().lower()
            enabled = bool(getattr(item, "enabled", True))
            raw_mode = getattr(item, "mode", None)
            if isinstance(raw_mode, str):
                mode = raw_mode.strip().lower() or None
        elif isinstance(item, str):
            preset_name = item.strip().lower()
        else:
            continue

        if not preset_name or preset_name not in allowed or preset_name in seen:
            continue
        if not enabled:
            continue
        if preset_name == "lru":
            continue
        seen.add(preset_name)
        ordered.append((preset_name, mode))
    return tuple(ordered)


class MultiScoreStrategy:
    name = "multi_score"

    def compute_score(
        self,
        *,
        key_id: str,
        config: Any,
        context: dict[str, Any],
    ) -> float | None:
        mode = str(getattr(config, "scheduling_mode", "lru") or "lru").strip().lower()
        if mode != "multi_score":
            return None

        all_key_ids = [str(k) for k in (context.get("all_key_ids") or []) if str(k)]
        if not all_key_ids:
            return None

        lru_scores = context.get("lru_scores", {})
        if not isinstance(lru_scores, dict):
            lru_scores = {}
        latency_avgs = context.get("latency_avgs", {})
        if not isinstance(latency_avgs, dict):
            latency_avgs = {}
        health_scores = context.get("health_scores", {})
        if not isinstance(health_scores, dict):
            health_scores = {}
        cost_totals = context.get("cost_totals", {})
        if not isinstance(cost_totals, dict):
            cost_totals = {}
        keys_by_id = context.get("keys_by_id", {})
        if not isinstance(keys_by_id, dict):
            keys_by_id = {}

        presets = _normalize_presets_from_config(config)
        lru_enabled = bool(getattr(config, "lru_enabled", True))
        if presets:
            return self._compute_preset_score(
                key_id=key_id,
                all_key_ids=all_key_ids,
                presets=presets,
                lru_enabled=lru_enabled,
                lru_scores=lru_scores,
                keys_by_id=keys_by_id,
            )

        weights = getattr(config, "scoring_weights", None)
        w_lru = safe_float(getattr(weights, "lru", 0.3)) or 0.0
        w_latency = safe_float(getattr(weights, "latency", 0.25)) or 0.0
        w_health = safe_float(getattr(weights, "health", 0.2)) or 0.0
        w_cost = safe_float(getattr(weights, "cost_remaining", 0.25)) or 0.0

        lru_rank = rank_ascending(key_id, lru_scores, all_key_ids)
        latency_rank = rank_ascending(key_id, latency_avgs, all_key_ids)

        health_raw = safe_float(health_scores.get(key_id))
        if health_raw is None:
            health_raw = 1.0
        health_norm = 1.0 - max(0.0, min(health_raw, 1.0))

        cost_limit = getattr(config, "cost_limit_per_key_tokens", None)
        used = safe_float(cost_totals.get(key_id)) or 0.0
        if cost_limit is None or int(cost_limit) <= 0:
            cost_norm = 0.0
        else:
            cost_norm = max(0.0, min(used / float(cost_limit), 1.0))

        return (
            w_lru * lru_rank
            + w_latency * latency_rank
            + w_health * health_norm
            + w_cost * cost_norm
        )

    def _compute_preset_score(
        self,
        *,
        key_id: str,
        all_key_ids: list[str],
        presets: tuple[tuple[str, str | None], ...],
        lru_enabled: bool,
        lru_scores: dict[str, Any],
        keys_by_id: dict[str, Any],
    ) -> float:
        lru_rank_asc = rank_ascending(key_id, lru_scores, all_key_ids)

        weighted_sum = 0.0
        weight_sum = 0.0

        for idx, (preset_name, mode) in enumerate(presets):
            metric = 0.5
            dim = get_preset_dimension(preset_name)
            if dim is not None:
                metric = dim.compute_metric(
                    key_id=key_id,
                    all_key_ids=all_key_ids,
                    keys_by_id=keys_by_id,
                    lru_scores=lru_scores,
                    mode=mode,
                )
            weight = 1.0 / (1.0 + _POSITIONAL_DECAY * idx)
            weighted_sum += metric * weight
            weight_sum += weight

        if weight_sum <= 0:
            return lru_rank_asc

        lru_blend = _LRU_BLEND_FACTOR if lru_enabled else 0.0
        preset_blend = 1.0 - lru_blend
        blended = (weighted_sum / weight_sum) * preset_blend + lru_rank_asc * lru_blend
        return max(0.0, min(blended, 1.0))


register_pool_strategy("multi_score", MultiScoreStrategy())


__all__ = [
    "MultiScoreStrategy",
]

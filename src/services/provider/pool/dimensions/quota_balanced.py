"""quota_balanced preset dimension."""

from __future__ import annotations

from typing import Any

from ._helpers import extract_usage_ratio, rank_ascending
from .registry import PresetDimensionBase, register_preset_dimension


class QuotaBalancedDimension(PresetDimensionBase):
    @property
    def name(self) -> str:
        return "quota_balanced"

    @property
    def label(self) -> str:
        return "额度平均"

    @property
    def description(self) -> str:
        return "优先选额度消耗最少的账号"

    def compute_metric(
        self,
        *,
        key_id: str,
        all_key_ids: list[str],
        keys_by_id: dict[str, Any],
        lru_scores: dict[str, Any],
        mode: str | None,
    ) -> float:
        usage_scores: dict[str, float] = {}
        for kid in all_key_ids:
            usage_ratio = extract_usage_ratio(keys_by_id.get(kid))
            if usage_ratio is not None:
                usage_scores[kid] = usage_ratio
        return rank_ascending(key_id, usage_scores, all_key_ids)


register_preset_dimension(QuotaBalancedDimension())

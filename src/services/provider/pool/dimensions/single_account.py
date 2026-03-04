"""single_account preset dimension."""

from __future__ import annotations

from typing import Any

from ._helpers import rank_descending
from .registry import PresetDimensionBase, register_preset_dimension


class SingleAccountDimension(PresetDimensionBase):
    @property
    def name(self) -> str:
        return "single_account"

    @property
    def label(self) -> str:
        return "单号优先"

    @property
    def description(self) -> str:
        return "集中使用同一账号（反向 LRU）"

    def compute_metric(
        self,
        *,
        key_id: str,
        all_key_ids: list[str],
        keys_by_id: dict[str, Any],
        lru_scores: dict[str, Any],
        mode: str | None,
    ) -> float:
        return rank_descending(key_id, lru_scores, all_key_ids)


register_preset_dimension(SingleAccountDimension())

"""Tests for PartitionKeySpec and the Glue projection parameters it builds."""

from __future__ import annotations

import pytest

from batch_event_job_monitor_cdk.partition_key_spec import (
    PartitionKeySpec,
    glue_partition_keys,
    partition_projection_parameters,
    regexp_extract_columns,
    storage_location_template_segment,
)


class TestPartitionKeySpecValidation:
    """Tests for PartitionKeySpec's __post_init__ validation."""

    def test_enum_projection_requires_enum_values(self) -> None:
        with pytest.raises(ValueError, match="enum_values"):
            PartitionKeySpec("job_type", "string", "enum")

    def test_date_projection_requires_date_range(self) -> None:
        with pytest.raises(ValueError, match="date_range"):
            PartitionKeySpec("year_month", "string", "date")

    def test_date_projection_requires_date_format(self) -> None:
        with pytest.raises(ValueError, match="date_format"):
            PartitionKeySpec(
                "year",
                "string",
                "date",
                date_range=("2020", "NOW"),
                date_interval_unit="YEARS",
            )

    def test_date_projection_requires_date_interval_unit(self) -> None:
        with pytest.raises(ValueError, match="date_interval_unit"):
            PartitionKeySpec(
                "year",
                "string",
                "date",
                date_range=("2020", "NOW"),
                date_format="yyyy",
            )

    def test_injected_projection_needs_no_values(self) -> None:
        key = PartitionKeySpec("tile_id", "string", "injected")
        assert key.enum_values is None

    def test_injected_projection_rejects_non_string_types(self) -> None:
        with pytest.raises(ValueError, match="requires glue_type 'string'"):
            PartitionKeySpec("tile_number", "int", "injected")


class TestPartitionProjectionParameters:
    """Tests for the projection.<key>.* Glue table parameters."""

    def test_enum_key(self) -> None:
        params = partition_projection_parameters(
            [PartitionKeySpec("job_type", "string", "enum", enum_values=("a", "b"))]
        )
        assert params == {
            "projection.job_type.type": "enum",
            "projection.job_type.values": "a,b",
        }

    def test_date_key(self) -> None:
        params = partition_projection_parameters(
            [
                PartitionKeySpec(
                    "year_month",
                    "string",
                    "date",
                    date_range=("2020-01", "NOW"),
                    date_format="yyyy-MM",
                    date_interval_unit="MONTHS",
                )
            ]
        )
        assert params == {
            "projection.year_month.type": "date",
            "projection.year_month.format": "yyyy-MM",
            "projection.year_month.range": "2020-01,NOW",
            "projection.year_month.interval": "1",
            "projection.year_month.interval.unit": "MONTHS",
        }

    def test_injected_key_emits_only_its_type(self) -> None:
        params = partition_projection_parameters(
            [PartitionKeySpec("tile_id", "string", "injected")]
        )
        assert params == {"projection.tile_id.type": "injected"}

    def test_mixed_keys(self) -> None:
        params = partition_projection_parameters(
            [
                PartitionKeySpec(
                    "job_type", "string", "enum", enum_values=("composite",)
                ),
                PartitionKeySpec("tile_id", "string", "injected"),
                PartitionKeySpec(
                    "year_month",
                    "string",
                    "date",
                    date_range=("2020-01", "NOW"),
                    date_format="yyyy-MM",
                    date_interval_unit="MONTHS",
                ),
            ]
        )
        assert params["projection.job_type.type"] == "enum"
        assert params["projection.tile_id.type"] == "injected"
        assert params["projection.year_month.type"] == "date"
        assert not any(key.startswith("projection.tile_id.values") for key in params)


class TestInjectedKeysInSharedHelpers:
    """An injected key is an ordinary partition key everywhere else."""

    _KEYS = [
        PartitionKeySpec("job_type", "string", "enum", enum_values=("composite",)),
        PartitionKeySpec("tile_id", "string", "injected"),
    ]

    def test_glue_partition_keys(self) -> None:
        columns = glue_partition_keys(self._KEYS)
        assert [(col.name, col.type) for col in columns] == [
            ("job_type", "string"),
            ("tile_id", "string"),
        ]

    def test_storage_location_template_segment(self) -> None:
        assert storage_location_template_segment(self._KEYS) == (
            "job_type=${job_type}/tile_id=${tile_id}/"
        )

    def test_regexp_extract_columns(self) -> None:
        sql = regexp_extract_columns(self._KEYS)
        assert "regexp_extract(key, '/tile_id=([^/]+)/', 1) AS tile_id" in sql

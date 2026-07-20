"""Partition key specification shared by the Athena database constructs.

A PartitionKeySpec describes one column of the ordered partition-key list
shared by the records/state/outputs Athena database constructs. The full
ordered list -- including the leading job_type entry, since job_type is just
another partition key at this layer -- drives the Glue partition-projection
parameters, the storage.location.template path, and the regexp_extract
columns of the state/outputs reconciliation views.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from aws_cdk import aws_glue as glue

# Glue partition projection requires an explicit date format and interval
# unit, neither of which is a PartitionKeySpec field. Both are inferred from
# the length of the date_range start value: "yyyy-MM" (7 chars) projects by
# MONTHS, "yyyy-MM-dd" (10 chars) projects by DAYS.
_DATE_FORMATS: dict[int, tuple[str, str]] = {
    len("yyyy-MM"): ("yyyy-MM", "MONTHS"),
    len("yyyy-MM-dd"): ("yyyy-MM-dd", "DAYS"),
}


@dataclass(frozen=True)
class PartitionKeySpec:
    """One partition key of a partition-projected Glue table.

    Parameters
    ----------
    name : str
        Partition key name. Used verbatim in the Glue partition key list,
        the storage.location.template path segments, and the
        regexp_extract column expressions in the generated view SQL.
    glue_type : str
        Glue/Athena column type of the partition key ("string", "date",
        etc.).
    projection : {"enum", "date"}
        Partition projection type.
    enum_values : tuple[str, ...] or None, optional
        Allowed partition values. Required when projection is "enum".
    date_range : tuple[str, str] or None, optional
        (start, end) partition projection range, e.g. ("2020-01", "NOW").
        Required when projection is "date".

    Raises
    ------
    ValueError
        If enum_values is missing for an "enum" projection, if date_range
        is missing for a "date" projection, or if a date_range start value
        does not match a supported date format.
    """

    name: str
    glue_type: str
    projection: Literal["enum", "date"]
    enum_values: tuple[str, ...] | None = None
    date_range: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if self.projection == "enum" and self.enum_values is None:
            raise ValueError(
                f"partition key '{self.name}': enum_values is required "
                "when projection is 'enum'"
            )
        if self.projection == "date":
            if self.date_range is None:
                raise ValueError(
                    f"partition key '{self.name}': date_range is required "
                    "when projection is 'date'"
                )
            if len(self.date_range[0]) not in _DATE_FORMATS:
                raise ValueError(
                    f"partition key '{self.name}': unsupported date_range "
                    f"start value '{self.date_range[0]}'"
                )


def glue_partition_keys(
    partition_keys: list[PartitionKeySpec],
) -> list[glue.CfnTable.ColumnProperty]:
    """Build the Glue partition-key column list.

    Parameters
    ----------
    partition_keys : list[PartitionKeySpec]
        Ordered partition key specs.

    Returns
    -------
    list[glue.CfnTable.ColumnProperty]
        One ColumnProperty per partition key, in order.
    """
    return [
        glue.CfnTable.ColumnProperty(name=key.name, type=key.glue_type)
        for key in partition_keys
    ]


def partition_projection_parameters(
    partition_keys: list[PartitionKeySpec],
) -> dict[str, str]:
    """Build the Glue "projection.<key>.*" table parameters.

    Parameters
    ----------
    partition_keys : list[PartitionKeySpec]
        Ordered partition key specs.

    Returns
    -------
    dict[str, str]
        Partition-projection parameters for every key, to merge into a
        table's ``parameters`` dict alongside ``projection.enabled``.
    """
    params: dict[str, str] = {}
    for key in partition_keys:
        if key.projection == "enum":
            assert key.enum_values is not None
            params[f"projection.{key.name}.type"] = "enum"
            params[f"projection.{key.name}.values"] = ",".join(key.enum_values)
        else:
            assert key.date_range is not None
            start, end = key.date_range
            date_format, interval_unit = _DATE_FORMATS[len(start)]
            params[f"projection.{key.name}.type"] = "date"
            params[f"projection.{key.name}.format"] = date_format
            params[f"projection.{key.name}.range"] = f"{start},{end}"
            params[f"projection.{key.name}.interval"] = "1"
            params[f"projection.{key.name}.interval.unit"] = interval_unit
    return params


def storage_location_template_segment(partition_keys: list[PartitionKeySpec]) -> str:
    """Build the "key=${key}/" path segment for storage.location.template.

    Parameters
    ----------
    partition_keys : list[PartitionKeySpec]
        Ordered partition key specs.

    Returns
    -------
    str
        Concatenated "{name}=${{{name}}}/" segments, one per partition key,
        in order.
    """
    return "".join(f"{key.name}=${{{key.name}}}/" for key in partition_keys)


def regexp_extract_columns(partition_keys: list[PartitionKeySpec]) -> str:
    """Build regexp_extract SELECT-list lines for each partition key.

    Parameters
    ----------
    partition_keys : list[PartitionKeySpec]
        Ordered partition key specs.

    Returns
    -------
    str
        Comma-separated "regexp_extract(key, '/{name}=([^/]+)/', 1) AS
        {name}" expressions, one per partition key, in order.
    """
    return ",\n            ".join(
        f"regexp_extract(key, '/{key.name}=([^/]+)/', 1) AS {key.name}"
        for key in partition_keys
    )

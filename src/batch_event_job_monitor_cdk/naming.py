"""Physical resource names built from a construct's ``name_prefix``."""

from __future__ import annotations

import re

from aws_cdk import Token

_LAMBDA_NAME_MAX_LENGTH = 64
_QUEUE_NAME_MAX_LENGTH = 80
_VALID_NAME = re.compile(r"\A[A-Za-z0-9_-]+\Z")


def _prefixed_name(
    name_prefix: str | None, suffix: str, *, max_length: int
) -> str | None:
    if name_prefix is None:
        return None
    name = f"{name_prefix}-{suffix}"
    # An unresolved token's length and characters are unknown until deploy.
    if Token.is_unresolved(name_prefix):
        return name
    if not _VALID_NAME.match(name_prefix):
        raise ValueError(
            f"name_prefix {name_prefix!r} must be non-empty and contain only "
            "letters, numbers, hyphens, and underscores"
        )
    if len(name) > max_length:
        raise ValueError(
            f"name {name!r} from name_prefix {name_prefix!r} is {len(name)} "
            f"characters, over the {max_length} character limit"
        )
    return name


def function_name(name_prefix: str | None, suffix: str) -> str | None:
    """Build a Lambda function name from a prefix.

    Parameters
    ----------
    name_prefix : str or None
        The construct's name_prefix.
    suffix : str
        What this function does, appended after the prefix.

    Returns
    -------
    str or None
        ``{name_prefix}-{suffix}``, or None to let CloudFormation generate
        the name.

    Raises
    ------
    ValueError
        If the prefix has invalid characters or the name exceeds Lambda's
        64 character limit.
    """
    return _prefixed_name(name_prefix, suffix, max_length=_LAMBDA_NAME_MAX_LENGTH)


def queue_name(name_prefix: str | None, suffix: str) -> str | None:
    """Build an SQS queue name from a prefix.

    Parameters
    ----------
    name_prefix : str or None
        The construct's name_prefix.
    suffix : str
        What this queue holds, appended after the prefix.

    Returns
    -------
    str or None
        ``{name_prefix}-{suffix}``, or None to let CloudFormation generate
        the name.

    Raises
    ------
    ValueError
        If the prefix has invalid characters or the name exceeds SQS's 80
        character limit.
    """
    return _prefixed_name(name_prefix, suffix, max_length=_QUEUE_NAME_MAX_LENGTH)

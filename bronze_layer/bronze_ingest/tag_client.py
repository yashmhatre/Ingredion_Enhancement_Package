"""
Transport for Unity Catalog tag assignments (#64).

Separate from `catalog_metadata.py` on purpose, and the separation is
transport-shaped rather than feature-shaped: the apply/diff logic lives beside
the COMMENT logic it mirrors, and only the wire call lives here. That is the
same split `databricks_fs.py` already makes for filesystem access, for the same
reason - a defensive `databricks-sdk` import belongs in one place.

**Why REST and not `ALTER TABLE ... SET TAGS`** (see
`docs/decisions/2026-08_uc_tag_mechanism.md`): the DDL raises `ParseException`
on OSS/local Delta, so a DDL implementation could never be executed - let alone
verified - by this package's test suite. Tag failures are non-fatal by design,
so an unverified implementation would silently report success while applying
nothing, which is worse for a governance feature than not shipping it. Behind
this Protocol a fake covers the logic, exactly as `MetadataDrafter` does for
the AI lane (#241).

Verified against the workspace 2026-08-12: entity types are `tables` and
`columns` (plural), and a column is addressed by four-part FQN
(`catalog.schema.table.column`), not a table name plus a column argument.
"""

from typing import Any, Dict, Optional, Protocol

from .logging_utils import logger

#: Entity type strings the assignment API expects. Plural, and passed
#: positionally - `TABLE`/`COLUMN` are rejected.
ENTITY_TABLE = "tables"
ENTITY_COLUMN = "columns"


class TagClient(Protocol):
    """
    Narrow, injectable interface between tag application and the workspace.

    Three methods, so a test double is a dict with no network, no SDK and no
    credentials. `catalog_metadata` never imports the SDK and never builds a
    URL; it diffs `list_tags` against what is configured and calls the other
    two for the difference.
    """

    def list_tags(self, entity_type: str, entity_name: str) -> Dict[str, str]:
        """Current {key: value} for one entity, or {} if it has none."""
        ...

    def set_tag(self, entity_type: str, entity_name: str, key: str, value: str) -> None:
        """Create or update one assignment. May raise; callers treat any
        exception as 'log and skip', never as fatal to the ingestion run."""
        ...

    def delete_tag(self, entity_type: str, entity_name: str, key: str) -> None:
        """Remove one assignment. Same failure contract as `set_tag`."""
        ...


def _workspace_client():
    """
    A databricks-sdk WorkspaceClient, or None if the SDK isn't importable.

    `databricks-sdk` is deliberately absent from `install_requires` - the
    runtime ships its own copy and pinning a second risks a version conflict on
    job compute (see setup.py's `sdk` extra). So this imports defensively and
    the caller degrades to a no-op, matching `databricks_fs.py`.
    """
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()
    except Exception:  # noqa: BLE001 - absent SDK is an expected state, not an error
        return None


class WorkspaceTagClient:
    """
    The real client. Wraps `w.entity_tag_assignments`.

    Constructed with `client=None` outside a workspace (or without the SDK),
    in which case every method no-ops and `list_tags` returns {} - so a
    non-UC environment applies nothing and reports nothing, rather than
    failing an ingestion run over governance metadata.
    """

    def __init__(self, client: Optional[Any] = None):
        self._client: Any = client if client is not None else _workspace_client()
        if self._client is None:
            logger.warning(
                "No Databricks SDK/workspace available - Unity Catalog tag assignment is "
                "disabled for this run. Tags are a UC feature; this is expected outside "
                "a workspace and is never fatal."
            )

    @property
    def available(self) -> bool:
        return self._client is not None

    def list_tags(self, entity_type: str, entity_name: str) -> Dict[str, str]:
        if self._client is None:
            return {}
        try:
            assignments = self._client.entity_tag_assignments.list(entity_type, entity_name)
            return {a.tag_key: (a.tag_value or "") for a in assignments}
        except Exception as exc:  # noqa: BLE001 - see module docstring
            logger.warning("Could not read tags for %s %s: %s", entity_type, entity_name, exc)
            return {}

    def set_tag(self, entity_type: str, entity_name: str, key: str, value: str) -> None:
        if self._client is None:
            return
        self._client.entity_tag_assignments.create(
            entity_type=entity_type,
            entity_name=entity_name,
            tag_key=key,
            tag_value=value,
        )

    def delete_tag(self, entity_type: str, entity_name: str, key: str) -> None:
        if self._client is None:
            return
        self._client.entity_tag_assignments.delete(entity_type, entity_name, key)

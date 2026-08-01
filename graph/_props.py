"""
graph/_props.py  –  Shared property-value sanitization for file-based targets
(gml.py, graphml.py).

Both GML and GraphML infer/declare a property's type from the value(s) they
see and reject (GML: Gephi) or silently duplicate (GraphML: two <key>
elements with the same attr.name but different types) any subsequent value
of a different type. But the same property name is shared across different
entity mappings in this pipeline, and different entities can back it with
different source column types — e.g. one hub's own key column is a Long
while another's is a padded CHAR string. Since properties are written one
entity at a time with no view of what other entities will contribute under
the same name, the only way to guarantee a single consistent type per name
across the whole graph is to normalize every scalar value to a string up
front. This also covers DB-native types psycopg2 returns for DATE/TIMESTAMP/
NUMERIC columns (date/datetime/Decimal), which neither writer can serialize
natively.

None is dropped entirely (not coerced to "") rather than risk pinning an
otherwise-numeric-looking property to a null-derived value on the first row
that happens to be missing it.
"""

from __future__ import annotations

from typing import Any, Dict


def sanitize_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return value
    return str(value)


def sanitize_props(props: Dict[str, Any]) -> Dict[str, Any]:
    return {k: sanitize_value(v) for k, v in props.items() if v is not None}

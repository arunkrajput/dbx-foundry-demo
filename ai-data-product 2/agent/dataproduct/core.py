"""
The standard data product interface, backed by Unity Catalog on Azure Databricks.

Consumers (apps, notebooks, AI agents) only ever call:
    describe()  health()  lineage()  query()  aggregate()  as_agent_tools()

Everything else (where the table lives, its contract, quality, policies) is resolved
from the data product registry at runtime. Policies are NOT applied here: Unity Catalog
applies column masks based on the identity this client authenticates as.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

REGISTRY = "dp_dev.governance.data_product_registry"
HEALTH = "dp_dev.governance.product_health"
_IDENT = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){2}$")


@dataclass
class _Sql:
    """Thin wrapper over the Databricks SQL Statement Execution API."""
    ws: WorkspaceClient
    warehouse_id: str

    def run(self, statement: str, params: dict[str, Any] | None = None) -> list[dict]:
        resp = self.ws.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in (params or {}).items()],
            wait_timeout="30s",
        )
        if resp.status.state != StatementState.SUCCEEDED:
            msg = resp.status.error.message if resp.status.error else resp.status.state
            raise RuntimeError(f"SQL failed: {msg}")
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = (resp.result.data_array or []) if resp.result else []
        return [dict(zip(cols, r)) for r in rows]


def _as_list(v) -> list:
    """The SQL API returns ARRAY columns as JSON text."""
    if v is None:
        return []
    return json.loads(v) if isinstance(v, str) else list(v)


# System columns that operational (bronze) products expose alongside the contract columns
_BRONZE_SYSTEM_COLUMNS = [
    {"name": "_dq_status", "type": "STRING", "filterable": True, "allowed_values": ["passed", "failed"],
     "description": "Whether the row as received passed the contract"},
    {"name": "_dq_errors", "type": "ARRAY<STRING>", "description": "Contract rules the row broke"},
]


class DataProduct:
    @classmethod
    def discover(cls, ws: WorkspaceClient | None = None, warehouse_id: str | None = None) -> list["DataProduct"]:
        """Every non-retired product this identity can see in the registry, across all layers."""
        sql = _Sql(ws or WorkspaceClient(), warehouse_id or os.environ["DATABRICKS_WAREHOUSE_ID"])
        ids = [r["product_id"] for r in sql.run(
            f"SELECT product_id FROM {REGISTRY} WHERE status <> 'retired' ORDER BY layer DESC")]
        return [cls(pid, ws=sql.ws, warehouse_id=sql.warehouse_id) for pid in ids]

    def __init__(self, product_id: str, ws: WorkspaceClient | None = None, warehouse_id: str | None = None):
        self.sql = _Sql(ws or WorkspaceClient(), warehouse_id or os.environ["DATABRICKS_WAREHOUSE_ID"])
        reg = self.sql.run(f"SELECT * FROM {REGISTRY} WHERE product_id = :pid", {"pid": product_id})
        if not reg:
            raise LookupError(f"Data product {product_id!r} is not registered (or you lack access)")
        self.reg = reg[0]
        self.contract = yaml.safe_load(self.reg["contract_yaml"])
        self.id = product_id
        self.table = self.reg["output_table"]
        if not _IDENT.match(self.table):
            raise ValueError(f"Unexpected table name {self.table!r}")
        self.layer = self.reg.get("layer")
        self.product_type = self.reg.get("product_type")
        self.inputs = _as_list(self.reg.get("inputs"))
        schema = list(self.contract["schema"])
        if self.layer == "bronze":
            schema += _BRONZE_SYSTEM_COLUMNS
        self.columns = {c["name"]: c for c in schema}
        self.max_rows = int(self.contract.get("agent", {}).get("max_rows", 50))
        self.aggs = self.contract.get("agent", {}).get("allowed_aggregations", ["count"])

    # ------------------------------------------------------------------ standard interface
    def describe(self) -> dict:
        return {
            "product_id": self.id,
            "layer": self.layer,
            "product_type": self.product_type,
            "when_to_use": self.reg.get("usage"),
            "inputs": self.inputs,
            "version": self.reg["version"],
            "status": self.reg["status"],
            "owner": self.reg["owner"],
            "description": self.reg["description"],
            "columns": [
                {"name": c["name"], "type": c["type"], "description": c["description"],
                 "pii": bool(c.get("pii")), "allowed_values": c.get("allowed_values")}
                for c in self.columns.values()
            ],
            "note": "Some columns may be masked or NULL for your identity by Unity Catalog policy.",
        }

    def _own_health(self, product_id: str) -> dict:
        rows = self.sql.run(f"SELECT * FROM {HEALTH} WHERE product_id = :pid", {"pid": product_id})
        h = rows[0] if rows else {"product_id": product_id, "status": "unknown"}
        h["trustworthy"] = str(h.get("trustworthy")).lower() == "true"
        h.pop("inputs", None)
        return h

    def health(self) -> dict:
        """Own freshness/quality AND the health of every input product it is built from."""
        h = self._own_health(self.id)
        inputs = [self._own_health(pid) for pid in self.inputs]
        h["inputs"] = [{k: i.get(k) for k in ("product_id", "status", "trustworthy", "age_hours", "valid_ratio")}
                       for i in inputs]
        h["own_trustworthy"] = h["trustworthy"]
        h["trustworthy"] = h["trustworthy"] and all(i["trustworthy"] for i in inputs)
        return h

    def lineage(self) -> dict:
        return {"product": f"{self.id}@{self.reg['version']}", "layer": self.layer,
                "output_table": self.table, "inputs": self.inputs,
                "upstreams": _as_list(self.reg.get("upstreams"))}

    def query(self, filters: dict[str, str] | None = None, columns: list[str] | None = None,
              limit: int = 20) -> dict:
        """Row-level read. Only contract columns; filters only on `filterable` columns; capped rows."""
        cols = columns or list(self.columns)
        bad = [c for c in cols if c not in self.columns]
        if bad:
            raise ValueError(f"Unknown columns {bad}")
        where, params = self._where(filters or {})
        limit = max(1, min(int(limit), self.max_rows))
        rows = self.sql.run(f"SELECT {', '.join(cols)} FROM {self.table}{where} LIMIT {limit}", params)
        return {"product": f"{self.id}@{self.reg['version']}", "row_count": len(rows), "rows": rows}

    def aggregate(self, measure: str = "account_id", agg: str = "count",
                  group_by: str | None = None, filters: dict[str, str] | None = None) -> dict:
        if agg not in self.aggs:
            raise ValueError(f"agg must be one of {self.aggs}")
        if measure not in self.columns or (group_by and group_by not in self.columns):
            raise ValueError("measure/group_by must be contract columns")
        if group_by and not self.columns[group_by].get("filterable"):
            raise ValueError("group_by must be a filterable column")
        where, params = self._where(filters or {})
        sel = f"{group_by}, " if group_by else ""
        grp = f" GROUP BY {group_by} ORDER BY {group_by}" if group_by else ""
        rows = self.sql.run(f"SELECT {sel}{agg}({measure}) AS value FROM {self.table}{where}{grp}", params)
        return {"product": f"{self.id}@{self.reg['version']}", "agg": f"{agg}({measure})", "rows": rows}

    # ------------------------------------------------------------------ agent exposure
    def as_agent_tools(self) -> list[dict]:
        """Tool specs generated from the contract. Plain dicts: {name, description, parameters}."""
        slug = self.id.replace(".", "__")
        filterable = {n: c for n, c in self.columns.items() if c.get("filterable")}
        filter_props = {
            n: {"type": "string", "description": c["description"],
                **({"enum": c["allowed_values"]} if c.get("allowed_values") else {})}
            for n, c in filterable.items()
        }
        about = (f"{self.layer} {self.product_type} data product '{self.id}' v{self.reg['version']} "
                 f"({self.reg['description']} When to use: {self.reg.get('usage') or 'n/a'})")
        return [
            {"name": f"describe__{slug}", "description": f"Schema, meaning and owner of the {about}.",
             "parameters": {"type": "object", "properties": {}}},
            {"name": f"health__{slug}",
             "description": f"Freshness and quality of the {about}. Call before quoting any figures.",
             "parameters": {"type": "object", "properties": {}}},
            {"name": f"query__{slug}",
             "description": f"Read individual rows from the {about}. Max {self.max_rows} rows.",
             "parameters": {"type": "object", "properties": {
                 "filters": {"type": "object", "properties": filter_props, "additionalProperties": False},
                 "columns": {"type": "array", "items": {"type": "string", "enum": list(self.columns)}},
                 "limit": {"type": "integer", "minimum": 1, "maximum": self.max_rows}}}},
            {"name": f"aggregate__{slug}",
             "description": f"Counts/sums/averages over the {about}. Prefer this over reading rows.",
             "parameters": {"type": "object", "properties": {
                 "measure": {"type": "string", "enum": list(self.columns)},
                 "agg": {"type": "string", "enum": self.aggs},
                 "group_by": {"type": "string", "enum": list(filterable)},
                 "filters": {"type": "object", "properties": filter_props, "additionalProperties": False}},
                 "required": ["measure", "agg"]}},
        ]

    def dispatch(self, tool_name: str, arguments: str | dict) -> str:
        """Route an agent's function call to the right method; always returns JSON text."""
        args = json.loads(arguments) if isinstance(arguments, str) else (arguments or {})
        method = tool_name.split("__", 1)[0]
        try:
            fn = {"describe": self.describe, "health": self.health,
                  "query": self.query, "aggregate": self.aggregate}[method]
            return json.dumps(fn(**args), default=str)
        except Exception as e:  # tell the model what went wrong instead of crashing the run
            return json.dumps({"error": str(e)})

    # ------------------------------------------------------------------ helpers
    def _where(self, filters: dict[str, str]) -> tuple[str, dict]:
        params, clauses = {}, []
        for i, (col, val) in enumerate(filters.items()):
            if col not in self.columns or not self.columns[col].get("filterable"):
                raise ValueError(f"Cannot filter on {col!r}")
            params[f"p{i}"] = val
            clauses.append(f"{col} = :p{i}")
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

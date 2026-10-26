from __future__ import annotations

from data_products import descriptor


def test_c360_graph_has_8_statements_3_sources():
    graph = descriptor.get_graph("c360")
    statements = [n for n in graph.nodes if n.type == "statement"]
    sources = [n for n in graph.nodes if n.type == "source"]
    assert len(statements) == 8 and len(sources) == 3


def test_fct_c360_profile_is_the_product_node():
    graph = descriptor.get_graph("c360")
    product = [n for n in graph.nodes if n.is_product]
    assert [n.id for n in product] == ["fct_c360_profile"]


def test_c360_statements_are_materialized_tables():
    # Every c360 model is a Flink materialized_table (per dbt), so its metrics
    # are scoped by MT UID (REST-discovered), not a statement name. Source topics
    # aren't statements and carry no table_type.
    graph = descriptor.get_graph("c360")
    statements = [n for n in graph.nodes if n.type == "statement"]
    sources = [n for n in graph.nodes if n.type == "source"]
    assert statements and all(n.table_type == "materialized_table" for n in statements)
    assert all(n.table_type is None for n in sources)


def test_unknown_product_returns_none():
    assert descriptor.get_graph("nope") is None


def test_list_summaries_reports_statement_count_8():
    summaries = descriptor.list_summaries()
    c360 = next(s for s in summaries if s.id == "c360")
    assert c360.statement_count == 8

"""Tests for zeroflow.viz."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from zeroflow.viz import mermaid_to_html, workflow_to_mermaid

_WF: dict[str, Any] = {
    "workflow_name": "demo",
    "default_entry_node": "a",
    "default_error_node": "err",
    "nodes": {
        "a": {
            "handler": "a",
            "outputs": {"ok": [{"target_node": "b"}], "fail": [{"target_node": "err"}]},
        },
        "b": {
            "handler": "b",
            "outputs": {"back": [{"target_node": "a", "is_loopback": True}]},
        },
        "err": {"handler": "err", "outputs": {}},
    },
}


# ---------------------------------------------------------------------------
# workflow_to_mermaid
# ---------------------------------------------------------------------------


def test_renders_fenced_mermaid_by_default() -> None:
    out = workflow_to_mermaid(_WF)
    assert out.startswith("```mermaid\n")
    assert out.endswith("\n```")
    assert "flowchart TD" in out


def test_unfenced_output_has_no_backticks() -> None:
    out = workflow_to_mermaid(_WF, fenced=False)
    assert "```" not in out
    assert out.startswith("flowchart TD")


def test_entry_node_uses_stadium_shape_and_error_uses_hexagon() -> None:
    out = workflow_to_mermaid(_WF)
    assert 'a(["a"])' in out
    assert 'err{{"err"}}' in out
    assert 'b["b"]' in out


def test_loopback_edge_uses_dotted_arrow() -> None:
    out = workflow_to_mermaid(_WF)
    assert "b -.->|back| a" in out
    assert "a -->|ok| b" in out


def test_active_and_done_styles_are_applied() -> None:
    out = workflow_to_mermaid(_WF, done_nodes=["a"], active_node="b")
    assert "a(" in out and ":::done" in out
    assert "b[" in out and ":::active" in out
    assert "err{" in out and ":::pending" in out


def test_failed_flag_flips_active_to_error_style() -> None:
    out = workflow_to_mermaid(_WF, active_node="b", failed=True)
    assert "b[" in out and ":::error" in out
    assert ":::active" not in out


def test_edges_without_target_are_skipped() -> None:
    wf: dict[str, Any] = {
        "workflow_name": "x",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": [{"target_node": None}, {"target_node": "b"}]},
            },
            "b": {"handler": "b", "outputs": {}},
        },
    }
    out = workflow_to_mermaid(wf)
    assert "a -->|ok| b" in out
    assert "None" not in out


# ---------------------------------------------------------------------------
# mermaid_to_html — offline rendering via the vendored mermaid.min.js bundle
# ---------------------------------------------------------------------------


def test_mermaid_to_html_writes_html_file_with_source_and_initializer(
    tmp_path: Path,
) -> None:
    source = "flowchart TD\n    A --> B"
    written = mermaid_to_html(source, tmp_path / "graph.html")

    assert written == tmp_path / "graph.html"
    html = written.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html
    assert '<pre class="mermaid">' in html
    assert "A --&gt; B" in html  # source is HTML-escaped
    assert "mermaid.initialize({ startOnLoad: true });" in html


def test_mermaid_to_html_strips_fenced_block_before_embedding(
    tmp_path: Path,
) -> None:
    fenced = "```mermaid\nflowchart TD\n    A --> B\n```"
    mermaid_to_html(fenced, tmp_path / "g.html")

    html = (tmp_path / "g.html").read_text(encoding="utf-8")
    assert "```" not in html
    assert "flowchart TD" in html


def test_mermaid_to_html_copies_local_mermaid_js_next_to_output(
    tmp_path: Path,
) -> None:
    mermaid_to_html("flowchart TD", tmp_path / "g.html")

    sibling_js = tmp_path / "mermaid.min.js"
    assert sibling_js.exists()
    assert sibling_js.stat().st_size > 100_000  # real bundle, not a stub
    html = (tmp_path / "g.html").read_text(encoding="utf-8")
    assert '<script src="mermaid.min.js"></script>' in html


def test_mermaid_to_html_does_not_re_copy_mermaid_js_if_already_present(
    tmp_path: Path,
) -> None:
    # Pre-seed the directory with a marker — mermaid_to_html must leave it
    # alone rather than blindly overwriting it.
    marker = b"PRE-EXISTING"
    (tmp_path / "mermaid.min.js").write_bytes(marker)

    mermaid_to_html("flowchart TD", tmp_path / "a.html")
    mermaid_to_html("flowchart TD", tmp_path / "b.html")

    assert (tmp_path / "mermaid.min.js").read_bytes() == marker


def test_mermaid_to_html_inlines_the_js_bundle_when_embed_js_true(
    tmp_path: Path,
) -> None:
    mermaid_to_html("flowchart TD", tmp_path / "g.html", embed_js=True)

    html = (tmp_path / "g.html").read_text(encoding="utf-8")
    # No external script reference; bundle is inlined inside the HTML.
    assert 'src="mermaid.min.js"' not in html
    assert not (tmp_path / "mermaid.min.js").exists()
    # The real bundle is ~3 MB; even a heavy-handed size check is safe.
    assert len(html) > 1_000_000


def test_mermaid_to_html_accepts_htm_extension(tmp_path: Path) -> None:
    written = mermaid_to_html("flowchart TD", tmp_path / "graph.htm")
    assert written.exists()


def test_mermaid_to_html_rejects_non_html_extensions(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported output extension"):
        mermaid_to_html("flowchart TD", tmp_path / "g.png")
    with pytest.raises(ValueError, match="unsupported output extension"):
        mermaid_to_html("flowchart TD", tmp_path / "g.svg")


def test_mermaid_to_html_uses_stem_as_title_when_none_provided(
    tmp_path: Path,
) -> None:
    mermaid_to_html("flowchart TD", tmp_path / "pipeline_01.html")

    html = (tmp_path / "pipeline_01.html").read_text(encoding="utf-8")
    assert "<title>pipeline_01</title>" in html
    assert "<h1>pipeline_01</h1>" in html


def test_mermaid_to_html_uses_provided_title_and_escapes_it(tmp_path: Path) -> None:
    mermaid_to_html(
        "flowchart TD",
        tmp_path / "g.html",
        title="<script>alert(1)</script>",
    )

    html = (tmp_path / "g.html").read_text(encoding="utf-8")
    # Raw tag must not appear; HTML-escaped form must.
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_mermaid_to_html_creates_missing_parent_directory(tmp_path: Path) -> None:
    nested = tmp_path / "out" / "deep"
    target = nested / "g.html"
    assert not nested.exists()

    mermaid_to_html("flowchart TD", target)

    assert target.exists()
    assert (nested / "mermaid.min.js").exists()

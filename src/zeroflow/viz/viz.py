"""Mermaid flowchart rendering for workflow definitions.

Two public entry points:

- :func:`workflow_to_mermaid` — pure-stdlib string generator that
  turns a workflow dict (the same shape accepted by
  ``WorkflowEngine``) into a ``flowchart TD`` Mermaid source. No I/O,
  no network, no subprocess.
- :func:`mermaid_to_html` — writes a self-contained HTML file that
  renders the diagram in any browser using the ``mermaid.js`` bundle
  shipped alongside this module (``mermaid.min.js``). Fully offline —
  no CDN, no third-party service, no subprocess, no CLI dependency.

The ``done_nodes`` / ``active_node`` / ``failed`` overlay on
``workflow_to_mermaid`` lets a caller repaint execution state on top
of the graph during a live run. Loopback edges (``is_loopback:
true``) render as dotted arrows so the wave boundary is visible.
"""

from __future__ import annotations

import html
from importlib.resources import files
from pathlib import Path
from typing import Any

_STYLE_PENDING = ":::pending"
_STYLE_ACTIVE = ":::active"
_STYLE_DONE = ":::done"
_STYLE_ERROR = ":::error"

_SUPPORTED_HTML_EXTENSIONS = (".html", ".htm")
_PACKAGE_JS_NAME = "mermaid.min.js"

_CLASS_DEFS = (
    "    classDef pending fill:#e2e8f0,stroke:#94a3b8,color:#475569",
    "    classDef active fill:#3b82f6,stroke:#1d4ed8,color:#fff,stroke-width:3px",
    "    classDef done fill:#22c55e,stroke:#16a34a,color:#fff",
    "    classDef error fill:#ef4444,stroke:#dc2626,color:#fff",
)


def workflow_to_mermaid(
    workflow_def: dict[str, Any],
    *,
    done_nodes: list[str] | None = None,
    active_node: str | None = None,
    failed: bool = False,
    fenced: bool = True,
) -> str:
    """Render a workflow definition as a Mermaid ``flowchart TD``.

    ``done_nodes`` and ``active_node`` paint execution state on top of
    the graph. ``failed`` switches the active node to the error style.
    ``fenced`` wraps the output in a ```` ```mermaid ```` fence; turn it
    off when writing a standalone ``.mmd`` file.
    """
    nodes = workflow_def.get("nodes", {})
    entry = workflow_def.get("default_entry_node", "")
    error_node = workflow_def.get("default_error_node", "")
    done = set(done_nodes or [])

    lines: list[str] = []
    if fenced:
        lines.append("```mermaid")
    lines.append("flowchart TD")

    for name in nodes:
        if name == entry:
            shape = f'{name}(["{name}"])'
        elif name == error_node:
            shape = f'{name}{{{{"{name}"}}}}'
        else:
            shape = f'{name}["{name}"]'

        if name == active_node:
            style = _STYLE_ERROR if failed else _STYLE_ACTIVE
        elif name in done:
            style = _STYLE_DONE
        else:
            style = _STYLE_PENDING

        lines.append(f"    {shape}{style}")

    for src, node_def in nodes.items():
        for out_name, targets in node_def.get("outputs", {}).items():
            for edge in targets:
                tgt = edge.get("target_node")
                if not tgt:
                    continue
                arrow = "-.->|" if edge.get("is_loopback") else "-->|"
                lines.append(f"    {src} {arrow}{out_name}| {tgt}")

    lines.append("")
    lines.extend(_CLASS_DEFS)
    if fenced:
        lines.append("```")

    return "\n".join(lines)


def mermaid_to_html(
    mermaid: str,
    output_path: str | Path,
    *,
    title: str | None = None,
    embed_js: bool = False,
) -> Path:
    """Write a self-contained HTML file that renders the Mermaid source.

    Uses the ``mermaid.min.js`` bundle shipped inside this package —
    no CDN, no network.

    Accepts both raw Mermaid and the fenced ``​```mermaid`` form
    produced by :func:`workflow_to_mermaid`; the fence is stripped
    automatically.

    Behaviour:

    - ``embed_js=False`` (default): writes ``mermaid.min.js`` next to
      ``output_path`` (if not already present) and the generated HTML
      loads it via ``<script src="mermaid.min.js">``. One JS file is
      shared by every HTML sibling rendered into the same directory —
      compact for batch runs (e.g. the tour).
    - ``embed_js=True``: inlines the full ``mermaid.min.js`` bundle
      inside a ``<script>`` tag in the HTML. Produces a single
      self-contained file at the cost of size (~3 MB per HTML).

    Output extension must be ``.html`` or ``.htm``.

    Raises:
        ValueError: ``output_path`` has an unsupported extension.
    """
    out = Path(output_path)
    suffix = out.suffix.lower()
    if suffix not in _SUPPORTED_HTML_EXTENSIONS:
        raise ValueError(
            f"unsupported output extension {out.suffix!r}; "
            f"use one of {', '.join(_SUPPORTED_HTML_EXTENSIONS)}"
        )

    source = _strip_mermaid_fence(mermaid)
    page_title = title if title is not None else out.stem

    out.parent.mkdir(parents=True, exist_ok=True)
    script_tag = _resolve_script_tag(out.parent, embed_js=embed_js)
    out.write_text(
        _build_html(title=page_title, mermaid_source=source, script_tag=script_tag),
        encoding="utf-8",
    )
    return out


def _resolve_script_tag(output_dir: Path, *, embed_js: bool) -> str:
    """Return the ``<script>`` tag that loads (or inlines) mermaid.js."""
    if embed_js:
        js = _package_js_bytes().decode("utf-8")
        return f"<script>\n{js}\n</script>"

    js_path = output_dir / _PACKAGE_JS_NAME
    if not js_path.exists():
        js_path.write_bytes(_package_js_bytes())
    return f'<script src="{_PACKAGE_JS_NAME}"></script>'


def _package_js_bytes() -> bytes:
    """Read the vendored ``mermaid.min.js`` bundle from this package."""
    return (files(__package__) / _PACKAGE_JS_NAME).read_bytes()


def _build_html(*, title: str, mermaid_source: str, script_tag: str) -> str:
    safe_title = html.escape(title)
    safe_source = html.escape(mermaid_source)
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        f"<title>{safe_title}</title>\n"
        "<style>\n"
        "  body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; color: #0f172a; }\n"
        "  h1 { font-size: 1.25rem; font-weight: 600; margin: 0 0 1.25rem 0; color: #334155; }\n"
        "  pre.mermaid { display: flex; justify-content: center; background: #fff; margin: 0; }\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        f"<h1>{safe_title}</h1>\n"
        f'<pre class="mermaid">\n{safe_source}\n</pre>\n'
        f"{script_tag}\n"
        "<script>mermaid.initialize({ startOnLoad: true });</script>\n"
        "</body>\n"
        "</html>\n"
    )


def _strip_mermaid_fence(text: str) -> str:
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines)

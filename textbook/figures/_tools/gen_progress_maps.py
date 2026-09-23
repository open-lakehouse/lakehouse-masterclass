#!/usr/bin/env python3
"""Generate per-chapter progress-map SVGs with consistent styling.

Each map shows the 11 milestones (Foundations + 10 layers). A milestone is:
  - "done"    -> green with a check
  - "current" -> amber, "you are here"
  - "todo"    -> dimmed slate

Usage: python3 gen_progress_maps.py   (writes fig-N.M-progress-*.svg files)
"""

MILESTONES = [
    "Foundations", "Setup", "Storage", "Compute", "Tables &\nCatalog",
    "Ingestion", "Transform", "Streaming", "Orchestr.", "Serving", "AI", "Agents",
]

# palette
BG = "#0f172a"
DONE_FILL, DONE_STROKE, DONE_TEXT = "#0f3d2e", "#4ade80", "#d1fae5"
CUR_FILL, CUR_STROKE, CUR_TEXT = "#3f2d1a", "#fbbf24", "#fef3c7"
TODO_FILL, TODO_STROKE, TODO_TEXT = "#1e293b", "#334155", "#64748b"


def chip(x, y, w, h, label, state):
    if state == "done":
        fill, stroke, text = DONE_FILL, DONE_STROKE, DONE_TEXT
    elif state == "current":
        fill, stroke, text = CUR_FILL, CUR_STROKE, CUR_TEXT
    else:
        fill, stroke, text = TODO_FILL, TODO_STROKE, TODO_TEXT
    sw = 3 if state in ("done", "current") else 1.5
    parts = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" '
             f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>']
    lines = [ln.replace("&", "&amp;") for ln in label.split("\n")]
    ty = y + h/2 - (len(lines)-1)*9 + 5
    for i, ln in enumerate(lines):
        parts.append(f'<text x="{x+w/2}" y="{ty+i*18}" fill="{text}" '
                     f'font-size="14" font-weight="700" text-anchor="middle">{ln}</text>')
    if state == "done":
        cx, cy = x + 16, y + 16
        parts.append(f'<circle cx="{cx}" cy="{cy}" r="9" fill="#22c55e"/>')
        parts.append(f'<path d="M{cx-4} {cy} l3 3 l6 -7" fill="none" '
                     f'stroke="#0f172a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>')
    if state == "current":
        parts.append(f'<text x="{x+w/2}" y="{y+h+16}" fill="{CUR_STROKE}" '
                     f'font-size="11" text-anchor="middle">you are here</text>')
    return "".join(parts)


def build(current_index, done_through_index, title):
    """done_through_index: milestones with index < this are 'done'."""
    W, H = 1280, 300
    title = title.replace("&", "&amp;")
    cols = 6
    cw, ch, gap = 180, 62, 14
    x0, y0 = 40, 96
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
           'font-family="Inter, Segoe UI, Helvetica, Arial, sans-serif">',
           f'<rect width="{W}" height="{H}" fill="{BG}"/>',
           f'<text x="{W/2}" y="52" fill="#f8fafc" font-size="26" font-weight="700" '
           f'text-anchor="middle">{title}</text>']
    for i, label in enumerate(MILESTONES):
        r, c = divmod(i, cols)
        x = x0 + c * (cw + gap)
        y = y0 + r * (ch + 34)
        state = "current" if i == current_index else ("done" if i < done_through_index else "todo")
        svg.append(chip(x, y, cw, ch, label, state))
    svg.append('</svg>')
    return "\n".join(svg)


TARGETS = [
    # (chapter_dir, filename, current_index, done_through_index, title)
    ("ch02", "fig-2.1-progress-setup.svg", 1, 1, "Progress, building: Setup"),
    ("ch02", "fig-2.7-progress-setup-done.svg", 2, 2, "Progress, Setup complete, Storage next"),
    ("ch03", "fig-3.1-progress-storage.svg", 2, 2, "Progress, building: Storage"),
    ("ch03", "fig-3.7-progress-storage-done.svg", 3, 3, "Progress, Storage complete, Compute next"),
    ("ch04", "fig-4.3-progress-compute.svg", 3, 3, "Progress — building: Compute"),
    ("ch04", "fig-4.4-progress-compute-done.svg", 4, 4, "Progress — Compute complete, Tables next"),
    ("ch05", "fig-5.3-progress-tables.svg", 4, 4, "Progress — building: Tables & Catalog"),
    ("ch05", "fig-5.4-progress-tables-done.svg", 5, 5, "Progress — Tables complete, Ingestion next"),
    ("ch06", "fig-6.3-progress-ingestion.svg", 5, 5, "Progress — building: Ingestion"),
    ("ch06", "fig-6.4-progress-ingestion-done.svg", 6, 6, "Progress — Ingestion complete, Transformation next"),
    ("ch07", "fig-7.3-progress-transform.svg", 6, 6, "Progress — building: Transformation"),
    ("ch07", "fig-7.4-progress-transform-done.svg", 7, 7, "Progress — Transformation complete, Streaming next"),
    ("ch08", "fig-8.3-progress-streaming.svg", 7, 7, "Progress — building: Streaming"),
    ("ch08", "fig-8.4-progress-streaming-done.svg", 8, 8, "Progress — Streaming complete, Orchestration next"),
    ("ch09", "fig-9.3-progress-orchestration.svg", 8, 8, "Progress — building: Orchestration"),
    ("ch09", "fig-9.4-progress-orchestration-done.svg", 9, 9, "Progress — Orchestration complete, Serving next"),
    ("ch10", "fig-10.3-progress-serving.svg", 9, 9, "Progress — building: Serving"),
    ("ch10", "fig-10.4-progress-serving-done.svg", 10, 10, "Progress — Serving complete, AI next"),
    ("ch11", "fig-11.3-progress-ai.svg", 10, 10, "Progress — building: AI"),
    ("ch11", "fig-11.4-progress-ai-done.svg", 11, 11, "Progress — AI complete, Agents next"),
    ("ch12", "fig-12.3-progress-agents.svg", 11, 11, "Progress — building: Agents"),
    ("ch12", "fig-12.4-progress-agents-done.svg", 12, 12, "Progress — Agents complete, Deploy next"),
    ("ch13", "fig-13.3-progress-complete.svg", -1, 12, "Progress — every layer built"),
    ("ch13", "fig-13.4-progress-complete.svg", -1, 12, "Your open lakehouse — complete"),
]

if __name__ == "__main__":
    import os
    # figures/ root is the parent of this _tools/ dir
    figures_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for chdir, fname, cur, done, title in TARGETS:
        out = build(cur, done, title)
        outdir = os.path.join(figures_root, chdir)
        os.makedirs(outdir, exist_ok=True)
        with open(os.path.join(outdir, fname), "w") as f:
            f.write(out)
        print("wrote", os.path.join(chdir, fname))

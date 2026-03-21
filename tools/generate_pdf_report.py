from __future__ import annotations

from pathlib import Path
import textwrap


PAGE_WIDTH = 595
PAGE_HEIGHT = 842
LEFT_MARGIN = 48
TOP_MARGIN = 60
BOTTOM_MARGIN = 50
LINE_HEIGHT = 14
FONT_SIZE = 10
MAX_CHARS = 92


def pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def paginate(lines: list[str]) -> list[list[str]]:
    max_lines_per_page = (PAGE_HEIGHT - TOP_MARGIN - BOTTOM_MARGIN) // LINE_HEIGHT
    return [lines[i : i + max_lines_per_page] for i in range(0, len(lines), max_lines_per_page)]


def build_text_lines(source_text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in source_text.splitlines():
        if not raw_line.strip():
            lines.append("")
            continue
        prefix = ""
        content = raw_line
        if raw_line.startswith("### "):
            prefix = "### "
            content = raw_line[4:]
        elif raw_line.startswith("## "):
            prefix = "## "
            content = raw_line[3:]
        elif raw_line.startswith("# "):
            prefix = "# "
            content = raw_line[2:]
        elif raw_line.startswith("- "):
            prefix = "- "
            content = raw_line[2:]
        elif raw_line.startswith("```"):
            lines.append(raw_line)
            continue
        wrapped = textwrap.wrap(content, width=MAX_CHARS - len(prefix)) or [""]
        for index, part in enumerate(wrapped):
            lines.append(f"{prefix if index == 0 else ' ' * len(prefix)}{part}".rstrip())
    return lines


def build_page_stream(page_lines: list[str]) -> bytes:
    commands = ["BT", f"/F1 {FONT_SIZE} Tf", f"1 0 0 1 {LEFT_MARGIN} {PAGE_HEIGHT - TOP_MARGIN} Tm"]
    for index, line in enumerate(page_lines):
        if index > 0:
            commands.append(f"0 -{LINE_HEIGHT} Td")
        commands.append(f"({pdf_escape(line)}) Tj")
    commands.append("ET")
    return "\n".join(commands).encode("cp1251", errors="replace")


def generate_pdf(source_path: Path, output_path: Path) -> None:
    lines = build_text_lines(source_path.read_text(encoding="utf-8"))
    pages = paginate(lines)

    objects: list[bytes] = []

    def add_object(payload: bytes | str) -> int:
        data = payload.encode("latin-1") if isinstance(payload, str) else payload
        objects.append(data)
        return len(objects)

    font_id = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    page_ids: list[int] = []
    content_ids: list[int] = []
    for page in pages:
        stream = build_page_stream(page)
        content_id = add_object(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        )
        content_ids.append(content_id)
        page_id = add_object(b"")
        page_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    pages_id = add_object(f"<< /Type /Pages /Count {len(page_ids)} /Kids [{kids}] >>")

    for idx, page_id in enumerate(page_ids):
        objects[page_id - 1] = (
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_ids[idx]} 0 R >>"
        ).encode("latin-1")

    catalog_id = add_object(f"<< /Type /Catalog /Pages {pages_id} 0 R >>")

    pdf = bytearray()
    pdf.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_position = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_position}\n%%EOF\n"
        ).encode("ascii")
    )
    output_path.write_bytes(pdf)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    generate_pdf(
        project_root / "docs" / "schema_notes.md",
        project_root / "docs" / "schema_report.pdf",
    )

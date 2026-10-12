"""Build the Persian customer guide DOCX and the ready-to-copy notebook."""
import json
from pathlib import Path

from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor


ROOT = Path(__file__).resolve().parent
FONT = "DejaVu Sans"
MONO = "DejaVu Sans Mono"


def set_rtl_paragraph(paragraph, rtl=True):
    ppr = paragraph._p.get_or_add_pPr()
    bidi = ppr.find(qn("w:bidi"))
    if bidi is None:
        bidi = OxmlElement("w:bidi")
        ppr.append(bidi)
    bidi.set(qn("w:val"), "1" if rtl else "0")
    paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT if rtl else WD_ALIGN_PARAGRAPH.LEFT


def set_run_font(run, font, size, rtl=False, bold=None, color=None):
    run.font.name = font
    run.font.size = Pt(size)
    if bold is not None:
        run.bold = bold
    if color:
        run.font.color.rgb = RGBColor(*color)
    rpr = run._r.get_or_add_rPr()
    fonts = rpr.rFonts
    if fonts is None:
        fonts = OxmlElement("w:rFonts")
        rpr.insert(0, fonts)
    for key in ("ascii", "hAnsi", "cs", "eastAsia"):
        fonts.set(qn(f"w:{key}"), font)
    rtl_el = rpr.find(qn("w:rtl"))
    if rtl:
        if rtl_el is None:
            rtl_el = OxmlElement("w:rtl")
            rpr.append(rtl_el)
        rtl_el.set(qn("w:val"), "1")
    elif rtl_el is not None:
        rtl_el.set(qn("w:val"), "0")


def shade(cell, fill):
    tcpr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), fill)
    tcpr.append(shd)


def set_cell_rtl(cell):
    tcpr = cell._tc.get_or_add_tcPr()
    direction = OxmlElement("w:textDirection")
    direction.set(qn("w:val"), "rlTb")
    tcpr.append(direction)
    for p in cell.paragraphs:
        set_rtl_paragraph(p)


def append_code(doc, lines):
    p = doc.add_paragraph()
    set_rtl_paragraph(p, False)
    p.paragraph_format.left_indent = Inches(.25)
    p.paragraph_format.right_indent = Inches(.25)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_together = True
    run = p.add_run("\n".join(lines))
    set_run_font(run, MONO, 9, color=(35, 49, 66))


def add_table(doc, rows):
    parsed = [[part.strip() for part in row.strip().strip("|").split("|")] for row in rows]
    parsed = [row for row in parsed if not all(set(cell) <= set("-: ") for cell in row)]
    table = doc.add_table(rows=0, cols=len(parsed[0]))
    table.style = "Light Shading Accent 1"
    table.autofit = True
    for ri, row in enumerate(parsed):
        cells = table.add_row().cells
        for ci, value in enumerate(row):
            value = value.replace("`", "")
            cells[ci].vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            set_cell_rtl(cells[ci])
            if ri == 0:
                shade(cells[ci], "DCEAF2")
            p = cells[ci].paragraphs[0]
            p.paragraph_format.space_after = Pt(3)
            r = p.add_run(value)
            set_run_font(r, FONT, 10, rtl=True, bold=(ri == 0), color=(22, 53, 74))
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def add_rtl_text(doc, text, style="Normal"):
    p = doc.add_paragraph(style=style)
    set_rtl_paragraph(p)
    p.paragraph_format.keep_together = style != "Normal" or text.startswith("برای انتخاب همه")
    if text.startswith("- "):
        p.style = "List Bullet"
        text = text[2:]
        set_rtl_paragraph(p)
    size = 21 if style == "Title" else 15 if style == "Heading 1" else 12 if style == "Heading 2" else 10.5
    for index, piece in enumerate(text.split("`")):
        if not piece:
            continue
        run = p.add_run(piece)
        if index % 2:
            set_run_font(run, MONO, max(8.5, size - 1), rtl=False, color=(35, 49, 66))
        else:
            set_run_font(run, FONT, size, rtl=True, bold=style != "Normal", color=(0, 0, 0))
    return p


def build_docx():
    source = (ROOT / "docs/customer_guide_fa.md").read_text(encoding="utf-8")
    doc = Document()
    section = doc.sections[0]
    section.page_width = Inches(8.5)
    section.page_height = Inches(11)
    section.top_margin = Inches(.62)
    section.bottom_margin = Inches(.58)
    section.left_margin = Inches(.62)
    section.right_margin = Inches(.62)
    section.header_distance = Inches(.3)
    section.footer_distance = Inches(.3)

    normal = doc.styles["Normal"]
    normal.font.name = FONT
    normal.font.size = Pt(11)
    normal.font.color.rgb = RGBColor(35, 43, 52)
    normal.paragraph_format.space_after = Pt(4)
    normal.paragraph_format.line_spacing = 1.05
    for style_name, size in (("Title", 21), ("Heading 1", 15), ("Heading 2", 12)):
        style = doc.styles[style_name]
        style.font.name = FONT
        style.font.size = Pt(size)
        style.font.bold = True
        style.font.color.rgb = RGBColor(0, 0, 0)
        style.paragraph_format.space_before = Pt(11 if style_name != "Title" else 0)
        style.paragraph_format.space_after = Pt(5)
        style.paragraph_format.keep_with_next = True

    lines = source.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith("```"):
            i += 1
            block = []
            while i < len(lines) and not lines[i].startswith("```"):
                block.append(lines[i])
                i += 1
            append_code(doc, block)
            i += 1
            continue
        if line.startswith("|"):
            table_lines = []
            while i < len(lines) and lines[i].startswith("|"):
                table_lines.append(lines[i])
                i += 1
            add_table(doc, table_lines)
            continue
        if line.startswith("# "):
            add_rtl_text(doc, line[2:], "Title")
        elif line.startswith("## "):
            add_rtl_text(doc, line[3:], "Heading 1")
        elif line.startswith("### "):
            add_rtl_text(doc, line[4:], "Heading 2")
        else:
            add_rtl_text(doc, line)
        i += 1

    footer = section.footer.paragraphs[0]
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = footer.add_run("راهنمای کاربر دفترچهٔ استخراج داده")
    set_run_font(run, FONT, 8, rtl=True, color=(90, 100, 110))
    doc.core_properties.title = "راهنمای پیدا کردن ستون‌ها و گرفتن خروجی"
    doc.core_properties.author = "Data Extractor"
    doc.core_properties.subject = "راهنمای فارسی کاربر دفترچه"
    doc.save(ROOT / "docs/customer_guide_fa.docx")


def build_notebook():
    cells = []

    def md(text):
        cells.append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(True)})

    def code(text):
        cells.append({"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text.splitlines(True)})

    md("# پیدا کردن ستون‌ها و گرفتن خروجی\nمدیر اتصال دفترچه را آماده کرده است. ابتدا تاریخ را وارد کنید و جدول نام و نوع ستون‌ها را ببینید؛ سپس شرط و خروجی را کامل کنید. راهنما: `docs/customer_guide_fa.docx`.")
    code('from data_extractor import Query\n\nquery = Query("customer_search")\nquery.between("2026-01-01", "2026-01-07")\ncolumns = query.show_columns()  # جدول فارسی نام و نوع ستون‌ها\n')
    md("## انتخاب شرط و خروجی\nنام‌های `customer_id` و `status` نمونه‌اند؛ پس از دیدن جدول بالا، آن‌ها را با نام‌های واقعی عوض کنید. این سلول را پس از سلول کشف ستون‌ها اجرا کنید. `limit` و `output_files` برای خروجی لازم‌اند.")
    code('from pyspark.sql.functions import col\n\nquery.where((col("customer_id") > 10) & (col("status") == "ready"))\nquery.select("customer_id", "status")\nquery.limit(100_000)\nquery.output_files(4)\nquery.run()\n')
    md("## راهنمای شرط‌ها\n`&` یعنی و، `|` یعنی یا، و `~` یعنی نقیض. هر مقایسه را در پرانتز بنویسید؛ چند `query.where(...)` با و ترکیب می‌شوند. تعداد فایل واقعی ممکن است با تعداد هدف فرق کند. انتخاب ردیف‌های `limit` ترتیب تضمین‌شده ندارد.")
    md("## دریافت\nفرمان دریافت چاپ‌شده در پیام پایان را روی دستگاه لینوکسی آماده اجرا کنید. تاریخ فرمان، تاریخ اجرای تهران است. پس از خطای مهلت، کار را پشت سر هم تکرار نکنید؛ قفل ممکن است تا توقف کار نگه داشته شود. شناسه و زمان خطا را به مدیر بدهید.")
    notebook = {
        "cells": cells,
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python", "version": "3.10"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    (ROOT / "notebooks/customer_example.ipynb").write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    build_docx()
    build_notebook()

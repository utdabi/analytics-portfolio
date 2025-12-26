# Reverse PDF Tool

Quick Python utility to reverse PDF page order using pypdf.

**Use case**: Flip document pages that were scanned in reverse order.

## Code

from pypdf import PdfReader, PdfWriter

reader = PdfReader("input.pdf")
writer = PdfWriter()

for page_num in range(len(reader.pages) - 1, -1, -1):
writer.add_page(reader.pages[page_num])

with open("output_reversed.pdf", 'wb') as out:
writer.write(out)

**Tech**: Python | pypdf
from typing import List
import os

from PyPDF2 import PdfFileMerger


def pdf_merger(pdfs: List, location_names: List, parent_names: List, levels: List, outfile: str):
    # how are inputs specified
    assert all([i.endswith('.pdf') for i in pdfs]), 'Not all files passed into `pdfs` are actual PDFs.'
    indir = '/'.join(pdfs[0].split('/')[:-1])

    # compile PDFs
    merger = PdfFileMerger()
    for i, (pdf, location_name, parent_name, level) in enumerate(zip(pdfs, location_names, parent_names, levels)):
        merger.append(pdf)
        if parent_name in location_names and level > 0:
            if parent_name == location_name:
                merger.addBookmark(f'{location_name} ', i, parent_name)
            else:
                merger.addBookmark(location_name, i, parent_name)
        else:
            merger.addBookmark(location_name, i)

    # get output file (if already exists, delete before writing new file)
    assert outfile.endswith('.pdf'), 'Provided output file is not a PDF.'
    if os.path.exists(outfile):
        os.remove(outfile)

    # write compiled PDF
    merger.write(outfile)
    merger.close()

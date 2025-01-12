from contextlib import contextmanager
from pathlib import Path
from typing import List

import dagster as dg
import pandas as pd
from pydantic import PrivateAttr
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.document_converter import DocumentConverter, PdfFormatOption


class PdfStatementParser(dg.ConfigurableResource):
    _doc_converter: DocumentConverter = PrivateAttr()
    dir_prefix: str

    @property
    def table_start(self):
        return 2

    @contextmanager
    def yield_for_execution(self, context: dg.InitResourceContext):
        pipeline_options = PdfPipelineOptions(do_table_structure=True, generate_page_images=True)
        pipeline_options.table_structure_options.do_cell_matching = True
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE

        self._doc_converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
        yield self

    def convert_doc(self, filename: Path) -> ConversionResult:
        source = Path(self.dir_prefix, filename)
        result = self._doc_converter.convert(source=source)
        return result


    def get_tables(self, result: ConversionResult) -> List[pd.DataFrame]:
        tables = [table.export_to_dataframe() for table in result.document.tables]
        return tables

    def combine_tables(self, tables: List[pd.DataFrame]) -> pd.DataFrame:
        tables_to_parse = tables[self.table_start :]
        while True:
            if len(tables_to_parse[-1].columns) == 2:
                _ = tables_to_parse.pop()
            else:
                break
        statement_df = pd.concat(tables_to_parse)
        return statement_df

    def get_statement_raw(self, source: Path) -> pd.DataFrame:
        result = self.convert_doc(source)
        tables = self.get_tables(result)
        statement_raw = self.combine_tables(tables)
        return statement_raw

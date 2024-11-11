import csv
import math
import os
from io import BytesIO
from zipfile import ZipFile

import chardet
import pandas as pd
import streamlit as st
import xlrd
import xlwt
from openpyxl import Workbook, load_workbook

def count_rows(file, file_extension):
    """Count actual data rows (excluding header) for different file formats."""
    file.seek(0)
    try:
        if file_extension in ['.csv', '.txt']:
            total_lines = sum(1 for _ in file)
            return total_lines - 1  # Exclude header
        elif file_extension == '.xlsx':
            wb = load_workbook(filename=BytesIO(file.read()), read_only=True)
            ws = wb.active
            total_rows = ws.max_row - 1  # Exclude header
            wb.close()
            return total_rows
        elif file_extension == '.xls':
            wb = xlrd.open_workbook(file_contents=file.read())
            sheet = wb.sheet_by_index(0)
            total_rows = sheet.nrows - 1  # Exclude header
            wb.release_resources()
            return total_rows
        else:
            raise ValueError("Unsupported file format")
    finally:
        file.seek(0)

def main():
    st.title("File Splitter")

    # Initialize session state
    if 'processed' not in st.session_state:
        st.session_state.processed = False
    if 'zip_buffer' not in st.session_state:
        st.session_state.zip_buffer = None
    if 'last_row_count' not in st.session_state:
        st.session_state.last_row_count = None
    if 'uploaded_file_name' not in st.session_state:
        st.session_state.uploaded_file_name = None

    file = st.file_uploader(
        "Choose a CSV, XLSX, XLS, or TXT file - WARNING: Large Files May Take Longer to Load.",
        type=['csv', 'xlsx', 'xls', 'txt']
    )

    if file is not None:
        try:
            # Reset state if new file
            if st.session_state.uploaded_file_name != file.name:
                st.session_state.processed = False
                st.session_state.zip_buffer = None
                st.session_state.uploaded_file_name = file.name

            row_count_split = st.number_input(
                "Enter the number of rows to split the file by",
                min_value=1,
                value=800000,
                key="row_count"
            )

            # Reset state if row count changes
            if st.session_state.last_row_count != row_count_split:
                st.session_state.processed = False
                st.session_state.zip_buffer = None

            st.session_state.last_row_count = row_count_split

            file_extension = os.path.splitext(file.name)[1].lower()

            # Get accurate row count excluding header
            data_rows = count_rows(file, file_extension)

            # Adjust the number of files calculation
            if data_rows % row_count_split == 0:
                num_files = data_rows // row_count_split
            else:
                num_files = (data_rows // row_count_split) + 1

            st.write(f"Number of files to be created: {num_files}")

            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                if file_extension in ['.csv', '.txt']:
                    try:
                        # Detect encoding and delimiter
                        sample = file.read(100000)
                        file.seek(0)
                        result = chardet.detect(sample)
                        detected_encoding = result['encoding'] if result['confidence'] > 0.8 else 'utf-8'
                        sample_str = sample.decode(detected_encoding, errors='replace')
                        delimiter = csv.Sniffer().sniff(sample_str).delimiter

                        zip_buffer = BytesIO()
                        with ZipFile(zip_buffer, 'w') as zip_file:
                            reader = pd.read_csv(
                                file,
                                chunksize=row_count_split,
                                encoding=detected_encoding,
                                sep=delimiter,
                                engine='python',
                                on_bad_lines='warn',
                                dtype=str
                            )

                            total_chunks = num_files
                            for i, chunk in enumerate(reader):
                                buffer = BytesIO()
                                chunk.to_csv(
                                    buffer,
                                    index=False,
                                    encoding=detected_encoding,
                                    sep=delimiter,
                                    lineterminator='\n'
                                )
                                buffer.seek(0)
                                filename = f"split_file_{i+1}{file_extension}"
                                zip_file.writestr(filename, buffer.read())
                                progress = (i + 1) / total_chunks
                                progress_bar.progress(min(progress, 1.0))
                    except Exception as e:
                        st.error(f"Error processing CSV/TXT file: {e}")
                        return

                elif file_extension == '.xlsx':
                    try:
                        wb = load_workbook(filename=BytesIO(file.read()), read_only=True)
                        ws = wb.active
                        header = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]

                        zip_buffer = BytesIO()
                        with ZipFile(zip_buffer, 'w') as zip_file:
                            row_buffer = []
                            file_index = 1
                            rows_processed = 0
                            total_rows_processed = 0

                            for row in ws.iter_rows(min_row=2, values_only=True):
                                row_buffer.append(row)
                                rows_processed += 1
                                total_rows_processed += 1

                                if rows_processed == row_count_split or total_rows_processed == data_rows:
                                    chunk_wb = Workbook()
                                    chunk_ws = chunk_wb.active
                                    chunk_ws.append(header)
                                    for data_row in row_buffer:
                                        chunk_ws.append(data_row)

                                    buffer = BytesIO()
                                    chunk_wb.save(buffer)
                                    buffer.seek(0)
                                    filename = f"split_file_{file_index}{file_extension}"
                                    zip_file.writestr(filename, buffer.read())
                                    buffer.close()
                                    chunk_wb.close()

                                    row_buffer = []
                                    rows_processed = 0
                                    file_index += 1
                                    progress = total_rows_processed / data_rows
                                    progress_bar.progress(min(progress, 1.0))

                        wb.close()

                    except Exception as e:
                        st.error(f"Error processing XLSX file: {e}")
                        return

                elif file_extension == '.xls':
                    try:
                        if row_count_split > 65536:
                            st.warning("Row count exceeds maximum for .xls (65,536 rows). Adjusting to 65,536.")
                            row_count_split = 65536

                        wb = xlrd.open_workbook(file_contents=file.read())
                        sheet = wb.sheet_by_index(0)
                        header = sheet.row_values(0)

                        zip_buffer = BytesIO()
                        with ZipFile(zip_buffer, 'w') as zip_file:
                            row_buffer = []
                            file_index = 1
                            rows_processed = 0
                            total_rows_processed = 0

                            for row_idx in range(1, sheet.nrows):
                                row_values = sheet.row_values(row_idx)
                                row_buffer.append(row_values)
                                rows_processed += 1
                                total_rows_processed += 1

                                if rows_processed == row_count_split or total_rows_processed == data_rows:
                                    chunk_wb = xlwt.Workbook()
                                    chunk_ws = chunk_wb.add_sheet('Sheet1')

                                    # Write header
                                    for col_num, value in enumerate(header):
                                        chunk_ws.write(0, col_num, value)

                                    # Write data rows
                                    for row_num, data_row in enumerate(row_buffer, 1):
                                        for col_num, value in enumerate(data_row):
                                            chunk_ws.write(row_num, col_num, value)

                                    buffer = BytesIO()
                                    chunk_wb.save(buffer)
                                    buffer.seek(0)
                                    filename = f"split_file_{file_index}{file_extension}"
                                    zip_file.writestr(filename, buffer.read())
                                    buffer.close()

                                    row_buffer = []
                                    rows_processed = 0
                                    file_index += 1
                                    progress = total_rows_processed / data_rows
                                    progress_bar.progress(min(progress, 1.0))

                        wb.release_resources()

                    except Exception as e:
                        st.error(f"Error processing XLS file: {e}")
                        return

                else:
                    st.error("Unsupported file format")
                    return

                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer
                zip_buffer.seek(0)

        except Exception as e:
            st.error(f"An error occurred during file splitting: {e}")

    if st.session_state.processed and st.session_state.zip_buffer is not None:
        st.download_button(
            label="Download All Split Files",
            data=st.session_state.zip_buffer.getvalue(),
            file_name="split_files.zip",
            mime='application/zip'
        )

if __name__ == "__main__":
    main()
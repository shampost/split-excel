import streamlit as st
import pandas as pd
import csv
import chardet
import os
import math
from io import BytesIO
from zipfile import ZipFile
from openpyxl import load_workbook, Workbook
import xlrd
import xlwt
import tempfile

# Initialize session state if not already initialized
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'zip_buffer' not in st.session_state:
    st.session_state.zip_buffer = None
if 'last_row_count' not in st.session_state:
    st.session_state.last_row_count = None
if 'uploaded_file_name' not in st.session_state:
    st.session_state.uploaded_file_name = None

# App title
st.title("File Splitter")

# File uploader
file = st.file_uploader(
    "Choose a CSV, XLSX, XLS, or TXT file",
    type=['csv', 'xlsx', 'xls', 'txt']
)

if file is not None:
    # Check if the uploaded file is different from the previous one
    if st.session_state.uploaded_file_name != file.name:
        # Reset session state if a new file is uploaded
        st.session_state.processed = False
        st.session_state.zip_buffer = None
        st.session_state.uploaded_file_name = file.name

    # Prompt for desired row count
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

    try:
        file_extension = os.path.splitext(file.name)[1].lower()
        if file_extension in ['.csv', '.txt']:
            # Read a sample to detect encoding
            file.seek(0)
            sample = file.read(min(file.size, 100000))
            file.seek(0)

            # Detect encoding
            result = chardet.detect(sample)
            detected_encoding = result['encoding']
            confidence = result['confidence']

            # If confidence is low or encoding is None, try common encodings
            if confidence < 0.8 or detected_encoding is None:
                encodings_to_try = ['utf-8-sig', 'utf-16', 'utf-16le', 'utf-16be', 'latin1', 'iso-8859-1']
                for encoding in encodings_to_try:
                    try:
                        file.seek(0)
                        pd.read_csv(file, nrows=5, encoding=encoding)
                        detected_encoding = encoding
                        break
                    except:
                        continue
                else:
                    detected_encoding = 'utf-8'  # Default fallback

            # Count total rows
            file.seek(0)
            total_rows = sum(1 for _ in pd.read_csv(
                file,
                encoding=detected_encoding,
                chunksize=8192,
                on_bad_lines='skip'
            )) - 1
            file.seek(0)

            num_files = (total_rows + row_count_split - 1) // row_count_split
            st.write(f"Number of files to be created: {num_files}")

            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                zip_buffer = BytesIO()
                with ZipFile(zip_buffer, 'w') as zip_file:
                    reader = pd.read_csv(
                        file,
                        chunksize=row_count_split,
                        encoding=detected_encoding,
                        on_bad_lines='skip'
                    )

                    for i, chunk in enumerate(reader):
                        buffer = BytesIO()
                        chunk.to_csv(
                            buffer,
                            index=False,
                            encoding=detected_encoding,
                            lineterminator='\n'  # Fixed parameter name
                        )
                        buffer.seek(0)
                        filename = f"split_file_{i+1}{file_extension}"
                        zip_file.writestr(filename, buffer.getvalue())
                        buffer.close()
                        progress_bar.progress((i + 1) / num_files)

                zip_buffer.seek(0)
                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer

        elif file_extension == '.xlsx':
            # Save uploaded file to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_input_file:
                tmp_input_file.write(file.getbuffer())
                tmp_input_file_path = tmp_input_file.name

            # Load workbook in read-only mode
            wb = load_workbook(filename=tmp_input_file_path, read_only=True)
            ws = wb.active

            # Get total number of rows (excluding header)
            total_rows = ws.max_row - 1
            num_files = (total_rows + row_count_split - 1) // row_count_split
            st.write(f"Number of files to be created: {num_files}")

            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                zip_buffer = BytesIO()
                with ZipFile(zip_buffer, 'w') as zip_file:
                    row_buffer = []
                    total_rows_processed = 0
                    file_index = 1

                    header = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]

                    for row in ws.iter_rows(min_row=2, values_only=True):
                        row_buffer.append(row)
                        total_rows_processed += 1

                        if len(row_buffer) == row_count_split or total_rows_processed == total_rows:
                            chunk_wb = Workbook()
                            chunk_ws = chunk_wb.active
                            chunk_ws.append(header)
                            for data_row in row_buffer:
                                chunk_ws.append(data_row)

                            buffer = BytesIO()
                            chunk_wb.save(buffer)
                            buffer.seek(0)
                            filename = f"split_file_{file_index}{file_extension}"
                            zip_file.writestr(filename, buffer.getvalue())
                            buffer.close()
                            chunk_wb.close()

                            row_buffer = []
                            file_index += 1
                            progress_bar.progress(total_rows_processed / total_rows)

                zip_buffer.seek(0)
                wb.close()
                os.remove(tmp_input_file_path)

                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer

        elif file_extension == '.xls':
            # Save uploaded file to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xls') as tmp_input_file:
                tmp_input_file.write(file.getbuffer())
                tmp_input_file_path = tmp_input_file.name

            # Open workbook
            wb = xlrd.open_workbook(tmp_input_file_path, on_demand=True)
            sheet = wb.sheet_by_index(0)

            # Get total number of rows (excluding header)
            total_rows = sheet.nrows - 1
            num_files = (total_rows + row_count_split - 1) // row_count_split
            st.write(f"Number of files to be created: {num_files}")

            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                zip_buffer = BytesIO()
                with ZipFile(zip_buffer, 'w') as zip_file:
                    total_rows_processed = 0
                    file_index = 1

                    header = sheet.row_values(0)

                    for start_row in range(1, sheet.nrows, row_count_split):
                        end_row = min(start_row + row_count_split, sheet.nrows)
                        chunk_wb = xlwt.Workbook()
                        chunk_ws = chunk_wb.add_sheet('Sheet1')

                        # Write header
                        for col_num, value in enumerate(header):
                            chunk_ws.write(0, col_num, value)

                        # Write data rows
                        for row_num, row_idx in enumerate(range(start_row, end_row), start=1):
                            row_values = sheet.row_values(row_idx)
                            for col_num, value in enumerate(row_values):
                                chunk_ws.write(row_num, col_num, value)
                            total_rows_processed += 1

                        buffer = BytesIO()
                        chunk_wb.save(buffer)
                        buffer.seek(0)
                        filename = f"split_file_{file_index}{file_extension}"
                        zip_file.writestr(filename, buffer.getvalue())
                        buffer.close()

                        file_index += 1
                        progress_bar.progress(total_rows_processed / total_rows)

                zip_buffer.seek(0)
                wb.release_resources()
                os.remove(tmp_input_file_path)

                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer

        else:
            st.error("Unsupported file format.")

    except Exception as e:
        st.error(f"Error processing file: {e}")

# Check if the files have been processed and display the download button
if st.session_state.processed:
    st.download_button(
        label="Download All Split Files",
        data=st.session_state.zip_buffer.getvalue(),
        file_name="split_files.zip",
        mime='application/zip'
    )
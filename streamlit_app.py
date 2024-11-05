import csv
import math  # For calculations
import os  # Added to handle file extensions
from io import BytesIO
from zipfile import ZipFile

import chardet
import pandas as pd
import streamlit as st
import xlrd  # For reading .xls files
import xlwt  # For writing .xls files
from openpyxl import Workbook, load_workbook  # For XLSX processing

# Initialize session state if not already initialized
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'zip_buffer' not in st.session_state:
    st.session_state.zip_buffer = None
if 'last_row_count' not in st.session_state:
    st.session_state.last_row_count = None
if 'uploaded_file_name' not in st.session_state:
    st.session_state.uploaded_file_name = None  # Initialize uploaded file name

# App title
st.title("File Splitter")

# File uploader
file = st.file_uploader(
    "Choose a CSV, XLSX, XLS, or TXT file - WARNING: Large Files May Take Longer to Load.",
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

    # Reset session state if a new row count is entered
    if st.session_state.last_row_count is not None and st.session_state.last_row_count != row_count_split:
        st.session_state.processed = False
        st.session_state.zip_buffer = None

    st.session_state.last_row_count = row_count_split  # Update the last row count

    # Determine file extension
    file_extension = os.path.splitext(file.name)[1].lower()

    if file_extension == '.csv' or file_extension == '.txt':
        try:
            # Read a sample to detect encoding and CSV format
            file.seek(0)
            sample_size = 100000  # Read first 100 KB for encoding detection and format sniffing
            sample = file.read(sample_size)
            file.seek(0)  # Reset file pointer after reading sample

            # Detect encoding
            result = chardet.detect(sample)
            detected_encoding = result['encoding']
            confidence = result['confidence']

            # If confidence is low or encoding is None, set a default encoding
            if confidence < 0.8 or detected_encoding is None:
                detected_encoding = 'utf-8'

            # Decode sample for CSV sniffer
            try:
                sample_str = sample.decode(detected_encoding)
            except UnicodeDecodeError:
                detected_encoding = 'utf-8'
                sample_str = sample.decode(detected_encoding, errors='replace')

            # Use CSV sniffer to detect delimiter and other parameters
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample_str)

            delimiter = dialect.delimiter

            # Estimate the number of files
            file.seek(0)
            reader = csv.reader((line.decode(detected_encoding) for line in file), delimiter=delimiter)
            total_rows = sum(1 for row in reader) - 1  # Subtract 1 for the header row
            file.seek(0)  # Reset file pointer
            num_files = math.ceil(total_rows / row_count_split)
            st.write(f"Number of files to be created: {num_files}")

            zip_buffer = BytesIO()
            with ZipFile(zip_buffer, 'w') as zip_file:
                try:
                    reader = pd.read_csv(
                        file,
                        chunksize=row_count_split,
                        encoding=detected_encoding,
                        sep=delimiter,
                        engine='python',  # Use python engine for better compatibility
                        on_bad_lines='warn',
                        dtype=str  # Read all columns as strings to preserve data
                    )
                except Exception as e:
                    st.error(f"Failed to read the file: {e}")
                    st.stop()

                # Process chunks
                progress_bar = st.progress(0)
                total_chunks = num_files
                for i, chunk in enumerate(reader):
                    try:
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
                        progress_bar.progress((i + 1) / total_chunks)
                    except Exception as e:
                        st.error(f"Error processing chunk {i+1}: {e}")
                        continue  # Skip to the next chunk
                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer
            zip_buffer.seek(0)

        except Exception as e:
            st.error(f"Error processing file: {e}")

    elif file_extension == '.xls':
        # Handle XLS files
        try:
            # Ensure row_count_split does not exceed 65,536
            if row_count_split > 65536:
                st.warning("Row count exceeds the maximum allowed for .xls files (65,536 rows). Adjusting to 65,536.")
                row_count_split = 65536

            # Read the file content once
            file.seek(0)
            file_content = file.read()

            # Open the workbook
            wb = xlrd.open_workbook(file_contents=file_content, on_demand=True)
            sheet = wb.sheet_by_index(0)  # Get the first sheet

            total_rows = sheet.nrows - 1  # Exclude header row
            num_files = math.ceil(total_rows / row_count_split)
            st.write(f"Number of files to be created: {num_files}")

            # Read header row
            header = sheet.row_values(0)

            # Add a button to confirm and start processing
            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                zip_buffer = BytesIO()
                with ZipFile(zip_buffer, 'w') as zip_file:
                    row_buffer = []
                    file_index = 1
                    rows_processed = 0
                    total_rows_processed = 0

                    for row_idx in range(1, sheet.nrows):  # Start from 1 to skip header
                        try:
                            row_values = sheet.row_values(row_idx)
                            row_buffer.append(row_values)
                            rows_processed += 1
                            total_rows_processed += 1

                            if rows_processed == row_count_split or total_rows_processed == total_rows:
                                # Write the chunk to a new XLS file
                                chunk_wb = xlwt.Workbook()
                                chunk_ws = chunk_wb.add_sheet('Sheet1')

                                # Write header
                                for col_num, header_value in enumerate(header):
                                    chunk_ws.write(0, col_num, header_value)

                                # Write data rows
                                for row_num, data_row in enumerate(row_buffer, start=1):
                                    for col_num, cell_value in enumerate(data_row):
                                        chunk_ws.write(row_num, col_num, cell_value)

                                # Save to a BytesIO buffer
                                buffer = BytesIO()
                                chunk_wb.save(buffer)
                                buffer.seek(0)
                                filename = f"split_file_{file_index}{file_extension}"
                                zip_file.writestr(filename, buffer.read())
                                buffer.close()
                                # Reset row buffer and counters
                                row_buffer = []
                                rows_processed = 0
                                file_index += 1
                                progress_bar.progress(file_index / (num_files + 1))
                        except Exception as e:
                            st.error(f"Error processing row {row_idx}: {e}")
                            continue  # Skip to the next row
                    zip_buffer.seek(0)
                wb.release_resources()
                del wb  # Cleanup

                # Store the ZIP file in session state
                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer

        except Exception as e:
            st.error(f"Error processing file: {e}")

    elif file_extension == '.xlsx':
        # Handle XLSX files
        try:
            # Read the file content once
            file.seek(0)
            file_content = file.read()

            # Open the workbook
            wb = load_workbook(filename=BytesIO(file_content), read_only=True)
            ws = wb.active  # Get the active sheet

            total_rows = ws.max_row - 1  # Exclude header row
            num_files = math.ceil(total_rows / row_count_split)
            st.write(f"Number of files to be created: {num_files}")

            # Read header row
            header = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]

            # Add a button to confirm and start processing
            if st.button("Confirm and Split File"):
                st.write("Splitting file... this might take some time")
                progress_bar = st.progress(0)

                zip_buffer = BytesIO()
                with ZipFile(zip_buffer, 'w') as zip_file:
                    row_buffer = []
                    file_index = 1
                    rows_processed = 0
                    total_rows_processed = 0

                    # Start from second row to skip header
                    for row in ws.iter_rows(min_row=2, values_only=True):
                        try:
                            row_buffer.append(row)
                            rows_processed += 1
                            total_rows_processed += 1

                            if rows_processed == row_count_split or total_rows_processed == total_rows:
                                # Write the chunk to a new XLSX file
                                chunk_wb = Workbook()
                                chunk_ws = chunk_wb.active
                                # Write header
                                chunk_ws.append(header)
                                for data_row in row_buffer:
                                    chunk_ws.append(data_row)
                                # Save to a BytesIO buffer
                                buffer = BytesIO()
                                chunk_wb.save(buffer)
                                buffer.seek(0)
                                filename = f"split_file_{file_index}{file_extension}"
                                zip_file.writestr(filename, buffer.read())
                                buffer.close()
                                chunk_wb.close()  # Close the workbook
                                # Reset row buffer and counters
                                row_buffer = []
                                rows_processed = 0
                                file_index += 1
                                progress_bar.progress(file_index / (num_files + 1))
                        except Exception as e:
                            st.error(f"Error processing row {total_rows_processed}: {e}")
                            continue  # Skip to the next row
                    zip_buffer.seek(0)
                wb.close()

                # Store the ZIP file in session state
                st.session_state.processed = True
                st.session_state.zip_buffer = zip_buffer

        except Exception as e:
            st.error(f"Error processing file: {e}")

    else:
        st.error("Unsupported file format")

# Check if the files have been processed and display the download button
if st.session_state.processed:
    st.download_button(
        label="Download All Split Files",
        data=st.session_state.zip_buffer.getvalue(),
        file_name="split_files.zip",
        mime='application/zip'
    )
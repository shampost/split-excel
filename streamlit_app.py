import pandas as pd
import streamlit as st
from io import BytesIO, StringIO
from zipfile import ZipFile
import chardet
import csv
import os  # Added to handle file extensions
from openpyxl import load_workbook, Workbook  # Added for XLSX processing
import math  # Added for calculations

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
        # Existing code for handling CSV and TXT files
        # (No changes needed here)
        # ...
        # [The CSV and TXT processing code remains the same]
        pass  # Placeholder for existing code

    elif file_extension == '.xlsx':
        # Existing code for handling XLSX files
        # (No changes needed here)
        # ...
        # [The XLSX processing code remains the same]
        pass  # Placeholder for existing code

    elif file_extension == '.xls':
        # Handle XLS files
        try:
            import xlrd  # Import xlrd for reading .xls files

            # Open the workbook
            file.seek(0)
            wb = xlrd.open_workbook(file_contents=file.read(), on_demand=True)
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
                        row_values = sheet.row_values(row_idx)
                        row_buffer.append(row_values)
                        rows_processed += 1
                        total_rows_processed += 1

                        if rows_processed == row_count_split or total_rows_processed == total_rows:
                            # Write the chunk to a new Excel file
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
                            filename = f"split_file_{file_index}.xlsx"
                            zip_file.writestr(filename, buffer.read())
                            buffer.close()
                            chunk_wb.close()  # Close the workbook
                            # Reset row buffer and counters
                            row_buffer = []
                            rows_processed = 0
                            file_index += 1
                            progress_bar.progress(total_rows_processed / total_rows)
                    zip_buffer.seek(0)
                wb.release_resources()
                del wb  # Cleanup

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
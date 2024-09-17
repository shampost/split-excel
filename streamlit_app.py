import pandas as pd
import streamlit as st
from io import BytesIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
    st.session_state.zip_buffer = None
    st.session_state.last_row_count = None  # Store the last row count used

# App title
st.title("Excel/CSV File Splitter")

# File uploader
file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])

if file is not None:
    # Prompt for desired row count
    row_count_split = st.number_input(
        "Enter the number of rows to split the file by", min_value=1, value=800000, key="row_count"
    )

    # Reset session state if a new row count is entered
    if st.session_state.last_row_count is not None and st.session_state.last_row_count != row_count_split:
        st.session_state.processed = False
        st.session_state.zip_buffer = None

    st.session_state.last_row_count = row_count_split  # Update the last row count

    try:
        # Read the file (auto-detect if it's CSV or Excel) using chunks to save memory
        if file.name.endswith('.csv'):
            file_iterator = pd.read_csv(file, chunksize=100000, low_memory=False)
        else:
            file_iterator = pd.read_excel(file, chunksize=row_count_split)

        # Create a ZIP file in memory, writing chunks directly to it
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zip_file:
            for i, chunk in enumerate(file_iterator):
                # Filter out empty rows in each chunk
                chunk = chunk.dropna(how='all')  # Drop rows where all values are NaN
                chunk = chunk[(chunk != '').any(axis=1)]  # Remove rows with empty strings

                # Write the chunk directly to the ZIP file as a CSV
                buffer = BytesIO()
                chunk.to_csv(buffer, index=False)
                buffer.seek(0)
                zip_file.writestr(f'split_file_{i+1}.csv', buffer.getvalue())

        zip_buffer.seek(0)

        # Store the ZIP file in session state
        st.session_state.processed = True
        st.session_state.zip_buffer = zip_buffer

    except Exception as e:
        st.error(f"Error processing file: {e}")
        st.stop()  # Safely stop execution if there's an error

# Check if the files have been processed and display the download button
if st.session_state.processed:
    st.download_button(
        label="Download All Split Files",
        data=st.session_state.zip_buffer,
        file_name="split_files.zip",
        mime="application/zip"
    )
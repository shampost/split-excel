import pandas as pd
import streamlit as st
from io import BytesIO
from zipfile import ZipFile

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
st.title("CSV File Splitter")

# File uploader
file = st.file_uploader("Choose a CSV file", type=['csv', 'xlsx'])

if file is not None:
    # Check if the uploaded file is different from the previous one
    if st.session_state.uploaded_file_name != file.name:
        # Reset session state if a new file is uploaded
        st.session_state.processed = False
        st.session_state.zip_buffer = None
        st.session_state.uploaded_file_name = file.name

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
        # Estimate the number of files
        total_rows = sum(1 for _ in file) - 1  # Subtract 1 for the header row
        file.seek(0)  # Reset file pointer
        num_files = (total_rows + row_count_split - 1) // row_count_split
        st.write(f"Number of files to be created: {num_files}")

        # Add a button to confirm and start processing
        if st.button("Confirm and Split File"):
            st.write("Splitting file... this might take some time")
            progress_bar = st.progress(0)

            zip_buffer = BytesIO()
            with ZipFile(zip_buffer, 'w') as zip_file:
                reader = pd.read_csv(file, chunksize=row_count_split)
                for i, chunk in enumerate(reader):
                    buffer = BytesIO()
                    chunk.to_csv(buffer, index=False)
                    buffer.seek(0)
                    filename = f"split_file_{i+1}.csv"
                    zip_file.writestr(filename, buffer.getvalue())
                    progress_bar.progress((i + 1) / num_files)
            zip_buffer.seek(0)

            # Store the ZIP file in session state
            st.session_state.processed = True
            st.session_state.zip_buffer = zip_buffer

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
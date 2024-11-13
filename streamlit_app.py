import pandas as pd
import streamlit as st
from io import BytesIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed
import chardet

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
    st.session_state.zip_buffer = None
    st.session_state.last_row_count = None  # Store the last row count used
    st.session_state.uploaded_file_name = None  # To track file changes

# App title
st.title("File Splitter")

# Cache the file loading function to avoid reloading it multiple times
@st.cache_data(show_spinner=True)
def load_file(file_bytes, file_name):
    """Load the file either CSV or Excel based on the file type"""
    file_buffer = BytesIO(file_bytes)
    file_extension = file_name.lower().split('.')[-1]

    if file_extension in ['csv', 'txt']:
        # Detect encoding
        result = chardet.detect(file_bytes)
        detected_encoding = result['encoding'] or 'utf-8'
        # Read CSV/TXT file
        return pd.read_csv(file_buffer, encoding=detected_encoding, on_bad_lines='skip')
    elif file_extension == 'xlsx':
        # Read XLSX file using openpyxl
        return pd.read_excel(file_buffer, engine='openpyxl')
    elif file_extension == 'xls':
        # Read XLS file using xlrd
        return pd.read_excel(file_buffer, engine='xlrd')
    else:
        raise ValueError("Unsupported file type")

# Cache the file chunk splitting process
@st.cache_data(show_spinner=True)
def split_file(df, row_count_split):
    """Splits the DataFrame into chunks and returns them"""
    total_rows = len(df)
    num_files = (total_rows + row_count_split - 1) // row_count_split
    chunks = [df.iloc[i:i + row_count_split] for i in range(0, total_rows, row_count_split)]
    return chunks, num_files

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

    # Read the uploaded file bytes
    file_bytes = file.getvalue()

    # Prompt for desired row count
    row_count_split = st.number_input(
        "Enter the number of rows to split the file by",
        min_value=1,
        value=800000,
        key="row_count"
    )

    # Reset session state if a new row count is entered
    if (st.session_state.last_row_count is not None and
            st.session_state.last_row_count != row_count_split):
        st.session_state.processed = False
        st.session_state.zip_buffer = None

    st.session_state.last_row_count = row_count_split  # Update the last row count

    try:
        # Load the file (from cache if possible)
        df = load_file(file_bytes, file.name)

        # Split the file into chunks and calculate the number of files (cached)
        chunks, num_files = split_file(df, row_count_split)

        st.write(f"Number of files to be created: {num_files}")

        # Add a button to confirm and start processing
        if st.button("Confirm and Split File"):
            st.write("Splitting file... this might take some time")
            progress_bar = st.progress(0)

            def process_chunk(i, chunk_df):
                buffer = BytesIO()
                # Decide file extension based on original file
                file_extension = file.name.lower().split('.')[-1]
                if file_extension in ['csv', 'txt']:
                    chunk_df.to_csv(buffer, index=False)
                    filename = f"split_file_{i+1}.{file_extension}"
                elif file_extension == 'xlsx':
                    chunk_df.to_excel(buffer, index=False, engine='openpyxl')
                    filename = f"split_file_{i+1}.xlsx"
                elif file_extension == 'xls':
                    chunk_df.to_excel(buffer, index=False, engine='xlwt')
                    filename = f"split_file_{i+1}.xls"
                else:
                    raise ValueError("Unsupported file type")
                buffer.seek(0)
                return filename, buffer.getvalue()

            # Create a ZIP file in memory
            zip_buffer = BytesIO()
            with ZipFile(zip_buffer, 'w') as zip_file:
                with ThreadPoolExecutor() as executor:
                    futures = []
                    for i, chunk in enumerate(chunks):
                        futures.append(executor.submit(process_chunk, i, chunk))

                    # Process the chunks and update the progress bar
                    for i, future in enumerate(as_completed(futures)):
                        filename, data = future.result()
                        zip_file.writestr(filename, data)
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
        data=st.session_state.zip_buffer,
        file_name="split_files.zip",
        mime="application/zip"
    )
import pandas as pd
import streamlit as st
from io import BytesIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed

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
st.title("Excel/CSV File Splitter")

# Cache the file loading function to avoid reloading it multiple times
@st.cache_data(show_spinner=True)
def load_file(file, file_name, file_bytes):
    """Load the file either CSV or Excel based on the file type"""
    file_buffer = BytesIO(file_bytes)
    if file_name.endswith('.csv'):
        return pd.read_csv(file_buffer)
    else:
        return pd.read_excel(file_buffer)

# Cache the file chunk splitting process
@st.cache_data(show_spinner=True)
def split_file(df, row_count_split):
    """Splits the DataFrame into chunks and returns them"""
    total_rows = len(df)
    num_files = (total_rows + row_count_split - 1) // row_count_split
    chunks = [df.iloc[i:i + row_count_split] for i in range(0, total_rows, row_count_split)]
    return chunks, num_files

# File uploader
file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])

if file is not None:
    # Check if the uploaded file is different from the previous one
    if st.session_state.uploaded_file_name != file.name:
        # Reset session state if a new file is uploaded
        st.session_state.processed = False
        st.session_state.zip_buffer = None
        st.session_state.uploaded_file_name = file.name

    # Load the entire file into memory to avoid issues with file reading in chunks
    file_bytes = file.read()

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
        # Load the file (from cache if possible)
        df = load_file(file, file.name, file_bytes)

        # Split the file into chunks and calculate the number of files (cached)
        chunks, num_files = split_file(df, row_count_split)

        st.write(f"Number of files to be created: {num_files}")

        # Add a button to confirm and start processing
        if st.button("Confirm and Split File"):
            st.write("Splitting file... this might take some time")
            progress_bar = st.progress(0)

            def process_chunk(i, chunk_df):
                buffer = BytesIO()
                # Write each chunk to CSV for smaller size and memory usage
                chunk_df.to_csv(buffer, index=False)
                buffer.seek(0)
                return f"split_file_{i+1}.csv", buffer.getvalue()

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
        data=st.session_state.zip_buffer.getvalue(),
        file_name="split_files.zip",
        mime='application/zip'
    )
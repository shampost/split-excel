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
            file_iterator = pd.read_csv(file, chunksize=row_count_split)
        else:
            file_iterator = pd.read_excel(file, chunksize=row_count_split)

        # Calculate the number of rows and files to be created
        total_rows = 0
        for chunk in file_iterator:
            total_rows += len(chunk)
        
        num_files = (total_rows + row_count_split - 1) // row_count_split
        st.write(f"Number of files to be created: {num_files}")

        # Reset the file iterator to start processing the chunks again
        if file.name.endswith('.csv'):
            file_iterator = pd.read_csv(file, chunksize=row_count_split)
        else:
            file_iterator = pd.read_excel(file, chunksize=row_count_split)

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
                    for i, chunk in enumerate(file_iterator):
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
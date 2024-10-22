import pandas as pd
import streamlit as st
from io import BytesIO, StringIO
from zipfile import ZipFile
import chardet
import csv

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
file = st.file_uploader("Choose a CSV file - WARNING: Large Files May Take Longer to Load.", type=['csv'])

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
        # st.write(f"Detected file encoding: {detected_encoding} with confidence {confidence*100:.2f}%")

        # If confidence is low or encoding is None, set a default encoding
        if confidence < 0.8 or detected_encoding is None:
            st.warning("Low confidence in detected encoding. Using 'utf-8' as default.")
            detected_encoding = 'utf-8'

        # Decode sample for CSV sniffer
        try:
            sample_str = sample.decode(detected_encoding)
        except UnicodeDecodeError:
            st.warning("Failed to decode sample with detected encoding. Using 'utf-8' as default.")
            detected_encoding = 'utf-8'
            sample_str = sample.decode(detected_encoding, errors='replace')

        # Use CSV sniffer to detect delimiter and other parameters
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_str)

        delimiter = dialect.delimiter
        # st.write(f"Detected delimiter: '{delimiter}'")

        # Estimate the number of files
        file.seek(0)
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
                for i, chunk in enumerate(reader):
                    buffer = BytesIO()
                    # Add BOM for UTF-16 encoding
                    if detected_encoding.lower() in ['utf-16', 'utf-16-le', 'utf-16-be']:
                        # Use 'utf-16' which automatically adds BOM
                        chunk.to_csv(
                            buffer,
                            index=False,
                            encoding=detected_encoding,
                            sep=delimiter,
                            lineterminator='\n'
                        )
                    else:
                        # For other encodings
                        chunk.to_csv(
                            buffer,
                            index=False,
                            encoding=detected_encoding,
                            sep=delimiter,
                            lineterminator='\n'
                        )
                    buffer.seek(0)
                    filename = f"split_file_{i+1}.csv"
                    zip_file.writestr(filename, buffer.read())
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
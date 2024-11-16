import streamlit as st

# Title for the app
st.title("File Upload Application")

# Description or instructions
st.write("Upload your files here, and we will process them!")

# File uploader
uploaded_file = st.file_uploader(
    "Choose a file", type=["txt", "csv", "xlsx", "png", "jpg", "pdf"]
)

# Handle uploaded file
if uploaded_file is not None:
    # Display file details
    st.write("File uploaded successfully!")
    st.write(f"**Filename:** {uploaded_file.name}")
    st.write(f"**File type:** {uploaded_file.type}")
    st.write(f"**File size:** {uploaded_file.size} bytes")

    # Display additional message or handle the file
    st.write("We'll process this file in the next steps.")

# Footer or additional info
st.write("---")
st.write("This is a simple file upload interface built with Streamlit!")

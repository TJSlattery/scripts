# MongoDB Image Storage Script

## Description

This Python script allows you to:
1. **Connect to a MongoDB Atlas database.**
2. **Read and process a directory of images recursively.**
3. **Compress images losslessly using zlib.**
4. **Store the binary image data in MongoDB with metadata.**
5. **Query the image from MongoDB.**
6. **Decompress and save the image to a new location.**

The script tracks and logs the time taken for each operation, ensuring visibility into performance for compression, storage, and retrieval processes.

---

## Features

- **Lossless Compression**: Images are compressed using `zlib` to preserve data quality.
- **Metadata Storage**: Each MongoDB document includes the original file name, directory path, and compressed image data.
- **Recursive Processing**: Reads all images from the specified directory and its subdirectories.
- **Query and Save**: Retrieve and decompress images from MongoDB, then save them to a specified location.
- **Performance Timing**: Measures and logs the execution time of major steps.

---

## Prerequisites

### Software Requirements
- Python 3.7 or higher
- MongoDB Atlas account with a connection string

### Python Libraries
Install the required libraries:
```bash
pip install pymongo pillow
```

---

## Usage

1. **Clone the Repository**
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Edit the Configuration**
   Open the script and update the following placeholders:
   - `MONGODB_URI`: Your MongoDB Atlas connection string stored as the environment variable **MONGODB_URI**.
   - `DATABASE_NAME`: Name of the database to store images.
   - `COLLECTION_NAME`: Name of the collection to store documents.
   - `IMAGE_DIRECTORY`: Path to the directory containing images.
   - `OUTPUT_DIRECTORY`: Path where queried images will be saved.

3. **Run the Script**
   ```bash
   python script.py
   ```

4. **Example Query**
   To query and save a specific image, modify this line in the script:
   ```python
   query = {"metadata.original_name": "example.jpg"}
   ```
   Replace `"example.jpg"` with the file name of the image you want to retrieve.

---

## MongoDB Document Structure

Each image is stored in MongoDB as a document with the following structure:
```json
{
  "file_path": "/path/to/image.jpg",
  "compressed_data": "<binary>",
  "metadata": {
    "original_name": "image.jpg",
    "directory": "/path/to"
  }
}
```

---

## Output Example

The script outputs logs detailing each step's performance:
```
Connected to MongoDB in 1.23 seconds
Compressed image /path/to/image1.jpg in 0.45 seconds
Inserted /path/to/image1.jpg into MongoDB in 0.12 seconds
Processed all images in 15.67 seconds
Queried MongoDB in 0.08 seconds
Decompressed image in 0.32 seconds
Saved image to /path/to/save/queried/images/image1.jpg in 0.05 seconds
```

---

## Customization

### Compression Format
Currently, the script saves images as JPEG before compressing with `zlib`. To change the format (e.g., to PNG):
1. Modify this line in the `compress_image_losslessly` function:
   ```python
   img.save(buffer, format="JPEG")
   ```
   Replace `"JPEG"` with `"PNG"` or another format.

### Query Filters
Modify the `query` object to filter images based on custom criteria (e.g., directory path or original name).

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

## Acknowledgments

- [Pillow (PIL)](https://pillow.readthedocs.io/) for image processing.
- [PyMongo](https://pymongo.readthedocs.io/) for MongoDB integration.
- [zlib](https://docs.python.org/3/library/zlib.html) for lossless compression.

---
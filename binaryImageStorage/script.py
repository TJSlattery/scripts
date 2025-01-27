import os
import zlib
import time
from pymongo import MongoClient
from pymongo.binary import Binary
from PIL import Image
import io

# Step 1: Connect to MongoDB Atlas
def connect_to_mongodb(uri, database_name, collection_name):
    start_time = time.time()
    client = MongoClient(uri)
    db = client[database_name]
    collection = db[collection_name]
    elapsed_time = time.time() - start_time
    print(f"Connected to MongoDB in {elapsed_time:.2f} seconds")
    return collection

# Step 2: Compress an image losslessly
def compress_image_losslessly(image_path):
    start_time = time.time()
    with Image.open(image_path) as img:
        img = img.convert("RGB")  # Ensure compatibility
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")  # Save as JPEG without quality loss
        compressed_data = zlib.compress(buffer.getvalue())  # Compress using zlib
    elapsed_time = time.time() - start_time
    print(f"Compressed image {image_path} in {elapsed_time:.2f} seconds")
    return compressed_data

# Step 3: Decompress an image
def decompress_image(compressed_data):
    start_time = time.time()
    decompressed_data = zlib.decompress(compressed_data)
    img = Image.open(io.BytesIO(decompressed_data))
    elapsed_time = time.time() - start_time
    print(f"Decompressed image in {elapsed_time:.2f} seconds")
    return img

# Step 4: Process images recursively and store in MongoDB
def process_images(directory, collection):
    total_time = time.time()
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')):
                file_path = os.path.join(root, file)
                try:
                    # Compress the image losslessly
                    compressed_data = compress_image_losslessly(file_path)

                    # Insert into MongoDB
                    start_time = time.time()
                    document = {
                        "file_path": file_path,
                        "compressed_data": Binary(compressed_data),
                        "metadata": {
                            "original_name": file,
                            "directory": root
                        }
                    }
                    collection.insert_one(document)
                    elapsed_time = time.time() - start_time
                    print(f"Inserted {file_path} into MongoDB in {elapsed_time:.2f} seconds")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    total_elapsed_time = time.time() - total_time
    print(f"Processed all images in {total_elapsed_time:.2f} seconds")

# Step 5: Query an image from MongoDB and save it to a new location
def query_and_save_image(collection, query, output_directory):
    start_time = time.time()
    document = collection.find_one(query)
    elapsed_time = time.time() - start_time
    print(f"Queried MongoDB in {elapsed_time:.2f} seconds")

    if not document:
        print("No document found for the given query.")
        return

    try:
        # Decompress the image
        compressed_data = document["compressed_data"]
        img = decompress_image(compressed_data)

        # Save the decompressed image
        original_name = document["metadata"]["original_name"]
        output_path = os.path.join(output_directory, original_name)
        
        save_start_time = time.time()
        img.save(output_path)
        save_elapsed_time = time.time() - save_start_time
        print(f"Saved image to {output_path} in {save_elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"Error saving image: {e}")

# Main function to run the script
if __name__ == "__main__":
    # MongoDB connection details
    MONGODB_URI = "your_mongodb_atlas_connection_string"
    DATABASE_NAME = "your_database_name"
    COLLECTION_NAME = "your_collection_name"

    # Directory containing images
    IMAGE_DIRECTORY = "/path/to/your/images"

    # Directory to save queried images
    OUTPUT_DIRECTORY = "/path/to/save/queried/images"

    # Connect to MongoDB
    collection = connect_to_mongodb(MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)

    # Process and store images
    process_images(IMAGE_DIRECTORY, collection)

    # Query and save an image
    query = {"metadata.original_name": "example.jpg"}  # Change this to match your query
    query_and_save_image(collection, query, OUTPUT_DIRECTORY)

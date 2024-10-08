# BigQuery MDM Implementation Using Embeddings

This project demonstrates a Master Data Management (MDM) implementation using BigQuery and embeddings. It showcases how to create a unified view of user profiles across multiple social media platforms using vector embeddings and similarity search.

## Author
Andrea Vasco (andrewankenobi@google.com)

## Features
- Generates synthetic user profiles and social media activity data
- Creates dimension and fact tables for multiple social media platforms
- Implements views to combine user data across platforms
- Uses BigQuery ML to generate embeddings for user profiles
- Demonstrates vector similarity search to find matching user profiles across platforms

## Requirements
- Google Cloud Platform account with BigQuery enabled
- Python 3.7+
- Google Cloud SDK
- BigQuery Python client library
- Vertex AI Python client library

## Setup
1. Clone this repository
2. Install the required Python libraries:
   ```
   pip install google-cloud-bigquery vertexai
   ```
3. Set up your Google Cloud credentials
4. Update the `DATASET_ID`, `CONNECTION_NAME`, and `EMBEDDING_MODEL` variables in the script if necessary

## Usage
Run the main script to generate data and create the MDM structure:
```
python datageneration.py
```

This will:
1. Generate synthetic user profiles and social media activity
2. Create dimension and fact tables in BigQuery
3. Create views to combine data across platforms
4. Generate embeddings for user profiles
5. Create a master record table with similarity scores

## Structure
- `datageneration.py`: Main script for data generation and BigQuery setup
- `README.md`: This file
- `LICENSE.md`: GNU General Public License

## License
This project is licensed under the GNU General Public License v3.0.

## Disclaimer
This is a sample implementation and should not be used in production without proper review and modifications to suit your specific needs and security requirements.
import pandas as pd
import json

def combine_features(row):
    # Combine artist, album, track name, and genre information for text-based recommendations
    return f"{row['artists']} {row['album_name']} {row['track_name']} {row['track_genre']}"

def process_spotify_csv(input_file, output_file):
    """
    Processes a Spotify tracks CSV file to create a VESPA-compatible JSON format.

    This function reads a CSV file consisting of Spotify Tracks Data. It processes the data to
    generate new columns to facilitate text search, and generates a JSON file as output with
    the necessary fields (`put` and `fields`) to index documents in Vespa and generate recommendations.
    
    Args:
      input_file (str): This is the path to the input CSV file consisting of Spotify track data.
                        Expected required columns include 'track_id', 'artists', 'album_name', 'track_name', and 'track_genre'.
      output_file (str): The path to the output JSON file to save the processed data in VESPA-Compatible Format.

    Workflow:
      1. Read and Load the CSV file into a Pandas DataFrame.
      2. Handle missing values in 'artists', 'album_name', 'track_name', and 'track_genre' columns by filling them with empty strings.
      3. Create a "text" column that combines specified features using the `combine_features` function for text-based recommendations.
      4. Select and rename columns to match required VESPA format: 'doc_id', 'title', and 'text'.
      5. Construct a JSON-like 'fields' column that includes the record's data as 'doc_id', 'title', and 'text'.
      6. Create a 'put' column based on 'doc_id' to uniquely identify each document with VESPA.
      7. Output the processed data to a JSON file in a Vespa-compatible format.

    Returns:
      None. Writes the processed DataFrame to `output_file` as a JSON file.
    """
    # Reading the Spotify Tracks dataset
    tracks = pd.read_csv(input_file)

    # Fill missing values in the required columns i.e. 'artists', 'album_name', 'track_name', 'track_genre'
    for f in ['artists', 'album_name', 'track_name', 'track_genre']:
        tracks[f] = tracks[f].fillna('')

    # Populating 'text' column for indexing by combining features
    tracks["text"] = tracks.apply(combine_features, axis=1)
    
    # Selecting the required columns only: 'track_id', 'track_name', 'text'
    tracks = tracks[['track_id', 'track_name', 'text']]
    tracks.rename(columns={'track_name': 'title', 'track_id': 'doc_id'}, inplace=True)

    # Creating 'fields' column as JSON-like structure of each record that resembles a dictionary in 'key':'value' pairs
    tracks['fields'] = tracks.apply(lambda row: row.to_dict(), axis=1)

    # Creating 'put' column based on 'doc_id' for indexing purpose
    tracks['put'] = tracks['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

    # Output the result to a JSON file in VESPA-Compatible Format
    df_result = tracks[['put', 'fields']]
    print(df_result.head())
    df_result.to_json(output_file, orient='records', lines=True)

# Calling function on our csv dataset
# The Output is a jsonl (JSON Lines) file which is compatible to facilitate ingestion, and indexing on VESPA
process_spotify_csv("spotify_dataset.csv", "clean_spotify.jsonl")

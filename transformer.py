import sys
import re
import zipfile
from typing import Generator, Dict, Tuple
import json
from datetime import datetime, timedelta
import csv

# Check if the correct number of arguments are provided
if len(sys.argv) != 2:
    print("Usage: python transformer.py <filename>")
    sys.exit(1)

# Get the path to the ZIP file from the command-line argument
zip_path = sys.argv[1]

print(f"The file you provided is: {zip_path}")

# Pattern to extract file names inside the 'data' folder ending with .json
pattern = r"data/(.+?)\.json"


def sort_files(file_list: list) -> list:
    """
    Sorts a list of files based on a numeric sequence extracted from their filenames.
    Example: '1_2_3.json' is split into [1, 2, 3] and used for sorting.
    """
    sorted_list = []
    for file_name in file_list:
        match = re.search(pattern, file_name)
        if not match:
            continue
        sequence = [int(x) for x in match.group(1).split("_")]
        sorted_list.append((sequence, match.group(1)))
    # Sort based on the numeric sequence
    sorted_list.sort(key=lambda x: x[0])
    return [file_name for _, file_name in sorted_list]


def files_content() -> Generator[Tuple[str, Dict], None, None]:
    """
    Generator function to read JSON files from the ZIP archive one by one,
    yielding the filename and parsed JSON data.
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # Get all file names inside the zip
        file_list = zip_ref.namelist()
        # Sort files before processing
        for file_name in sort_files(file_list):
            with zip_ref.open(f"data/{file_name}.json") as file:
                content = file.read()
                data = json.loads(content)
                yield file_name, data


# List to collect processed rows
rows = []

# Variables to hold match metadata and players info
match = None
players = None

# Process each file inside the ZIP
for index, file_content in enumerate(files_content()):
    name, data = file_content

    # First file contains match and players information
    if index == 0:
        match = data["match"]
        players = {player["team"]: player for player in match["players"]}

    # Extract sequences
    sequences = data["sequences"]

    # Lists to hold 'hit' and 'bounce' events separately
    hits, bounces = [], []
    for sample in data["samples"]:
        if sample.get("event") == "hit":
            hits.append(sample)
        if sample.get("event") == "bounce":
            bounces.append(sample)

    # Process each shot
    for shot in data["shots"]:
        shot_n = shot["shot_no"]

        # If shot number is greater than number of hits, skip
        if shot_n > len(hits):
            continue

        # Parse timestamps
        timestamp = shot["time_utc"]
        duration = shot["duration"]
        start_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_time = (start_time + timedelta(seconds=duration)).isoformat() + "Z"

        # Find corresponding hit
        hit = hits[shot_n - 1]

        # Find the bounce event that happens between the hit and the end of shot duration
        bounce = next(
            (x for x in bounces if hit["time"] < x["time"] < hit["time"] + duration),
            None,
        )

        # Sort players: hitter first, receiver second
        hit_players_positions = sorted(
            hit["players"], key=lambda x: x["team"] == shot["team"], reverse=True
        )

        # Get ball position at hit and bounce
        hit_ball = hit["ball"]["pos"]
        bounce_ball = bounce["ball"]["pos"] if bounce else {"x": "", "y": ""}

        # Create a row with all relevant shot data
        row = {
            "season": match["season"],
            "tournament_id": match["tournament_id"],
            "draw_code": match["draw_code"],
            "set": sequences["set"],
            "game": sequences["game"],
            "point": sequences["point"],
            "serve": sequences["serve"],
            "rally": sequences["rally"],
            "shot_n": shot_n,
            "hitter_external_id": players[shot["team"]]["external_id"],
            "stroke": shot["stroke"],
            "spin_type": shot["spin"]["type"],
            "spin_rpm": shot["spin"]["rpm"],
            "call": shot["call"],
            "shot_start_timestamp": timestamp,
            "shot_end_timestamp": end_time,
            "ball_hit_x": hit_ball["x"],
            "ball_hit_y": hit_ball["y"],
            "ball_hit_z": hit_ball["z"],
            "ball_bounce_x": bounce_ball["x"],
            "ball_bounce_y": bounce_ball["y"],
            "hitter_x": hit_players_positions[0]["pos"]["x"],
            "hitter_y": hit_players_positions[0]["pos"]["y"],
            "receiver_x": hit_players_positions[1]["pos"]["x"],
            "receiver_y": hit_players_positions[1]["pos"]["y"],
        }
        # Append the row to the final list
        rows.append(row)

print(f"Processed {len(rows)} rows from the files.")
# Write all processed rows into a CSV file
with open("result.csv", mode="w", newline="") as file:
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(rows)

print("Data has been written to result.csv")

UmaDump JSON Viewer
=====================

Overview
--------
This tool handles transforming and combining game data from Umamusume's master.mdb and
[Umadump](https://github.com/rockisch/umadump) JSON output (expected as
umadump_data.json in the project root) into consumable, easily browsable, human-
readable JSON files with a cleaned UMA list produced from sourced data.

Key capabilities
----------------
- Generate game_data JSONs (factor.json, skills.json, chara.json, races.json) from the
  local master.mdb SQLite database in read-only mode.
- Fall back to existing JSON files if the DB is not present (silent usage). If neither
  DB nor JSONs exist, the script raises an informative error.
- Produce a cleaned_umas_umadump.json output containing calculated ratings and
  normalized fields (stats, affinities, sparks, skills, parents, win counts, etc.) from
  umadump_data.json.
- Customizable rating algorithm for proprietary sorting of Umas based on various
  attributes.
- Helper functions to validate schema, map IDs to readable strings, and compute
  ratings.

Primary script
--------------
- spark_db_umadump.py
  - Ensures game_data JSONs are available (updates from DB when present).
  - Loads umadump_data.json and game_data JSONs and produces cleaned_umas_umadump.json.
  - Includes placeholder SQL queries for creating the JSONs from master.mdb; replace or
    extend those queries to match your local master.mdb schema.

Default DB lookup path
----------------------
%USERPROFILE%\AppData\LocalLow\Cygames\Umamusume\master\master.mdb

Behavior summary
----------------
- If master.mdb exists at the default path, the script will (re)create/update the
  game_data JSONs every run.
- If master.mdb does not exist but the JSONs already exist in game_data/, the script
  uses the JSONs without error.
- If neither the DB nor the required JSONs exist, the script raises FileNotFoundError
  with guidance.

Configuration
-------------
- Paths and filenames are defined near the top of spark_db_umadump.py (CONFIG
  dictionary). Update if you want different locations.

Usage
-----
1. Ensure Python 3.10+ is installed. No extra libraries are required.
2. Run `python spark_db_umadump.py`.
   This will (re)generate game_data JSONs and produce cleaned_umas_umadump.json from
   umadump_data.json.

Dependencies
------------
- Standard Python libraries only: sqlite3, json, pathlib, typing.

Troubleshooting
---------------
- If the script raises FileNotFoundError, either ensure the game is installed in the
  default location or ensure the required JSONs (factor.json, skills.json, chara.json,
  races.json) exist in the game_data/ directory.
- If JSON parsing fails, inspect the file for corruption or differing schema and update
  create_jsons_from_db accordingly.

License
-------
This repository is licensed under the MIT License. See the LICENSE file at the
repository root for the full text and conditions.

Notes on privacy & usage
------------------------
- These tools perform local read-only inspection of a game DB and generate JSON
  artifacts. Use them responsibly and in accordance with the game's terms of service.

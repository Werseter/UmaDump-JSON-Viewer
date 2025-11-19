import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

# --- Configurable paths ---
BASE_DIR = os.path.dirname(__file__)
GAME_DATA_DIR = os.path.join(BASE_DIR, 'game_data')
CONFIG = {
    'game_data_dir': GAME_DATA_DIR,
    'umadump_data': os.path.join(BASE_DIR, 'umadump_data.json'),
    'factor': os.path.join(GAME_DATA_DIR, 'factor.json'),
    'skills': os.path.join(GAME_DATA_DIR, 'skills.json'),
    'chara': os.path.join(GAME_DATA_DIR, 'chara.json'),
    'races': os.path.join(GAME_DATA_DIR, 'races.json'),
    'output': os.path.join(BASE_DIR, 'cleaned_umas_umadump.json')
}


def ensure_game_data_jsons_exist():
    """Ensure the expected game-data JSON files exist in the game_data subfolder.

    - If the local master.mdb database exists, always (re)create/update the game_data JSON files from it.
    - If the DB does not exist but the JSON files already exist, silently use the JSONs (no update needed).
    - If neither DB nor JSONs exist, raise FileNotFoundError explaining what's missing.
    """
    required = [CONFIG['factor'], CONFIG['skills'], CONFIG['chara'], CONFIG['races']]
    Path(CONFIG['game_data_dir']).mkdir(parents=True, exist_ok=True)

    # Default Windows path for the game's master DB
    db_path = Path.home() / "AppData" / "LocalLow" / "Cygames" / "Umamusume" / "master" / "master.mdb"
    db_exists = db_path.exists()

    if db_exists:
        # DB present: always update/create all game_data JSONs from the DB
        targets = [CONFIG['factor'], CONFIG['skills'], CONFIG['chara'], CONFIG['races']]
        create_jsons_from_db(db_path, targets)
        return

    # DB not present: only accept if JSON files already exist
    missing = [p for p in required if not os.path.exists(p)]
    if not missing:
        # JSONs exist -> nothing to do
        return

    # Neither DB nor full JSONs present -> error
    raise FileNotFoundError(
        f"Database not found at {db_path!s} and required JSON files missing: {', '.join(missing)}."
        f" Install the game at default location or provide the game_data JSONs in {CONFIG['game_data_dir']}.")


def create_jsons_from_db(db_path: Path, targets: list[str]):
    """Create JSON files from the local master.mdb SQLite database.

    This function opens the DB in read-only mode and runs queries to produce
    the same JSON artifacts the rest of the script expects.

    NOTE: Uses read-only sqlite connection via URI (mode=ro) to avoid any
    accidental writes.
    """
    # Open DB read-only
    conn = sqlite3.connect(f"file:{str(db_path)}?mode=ro", uri=True)
    try:
        cur = conn.cursor()

        if CONFIG['factor'] in (targets or []):
            cur.execute("SELECT td.[index], td.[text] FROM text_data td WHERE category = 147;")
            rows = cur.fetchall()
            factor_game_data = {str(r[0]): r[1] for r in rows}
            with open(CONFIG['factor'], 'w', encoding='utf-8') as fp:
                json.dump(factor_game_data, fp, ensure_ascii=False, indent=2)

        if CONFIG['skills'] in (targets or []):
            cur.execute("SELECT td.[index], td.[text] FROM text_data td WHERE category = 47;")
            rows = cur.fetchall()
            skills_game_data = {str(r[0]): r[1] for r in rows}
            with open(CONFIG['skills'], 'w', encoding='utf-8') as fp:
                json.dump(skills_game_data, fp, ensure_ascii=False, indent=2)

        if CONFIG['chara'] in (targets or []):
            cur.execute("SELECT td.[index], td.[text] FROM text_data td WHERE category = 4;")
            rows = cur.fetchall()
            chara_game_data = {str(r[0]): r[1] for r in rows}
            with open(CONFIG['chara'], 'w', encoding='utf-8') as fp:
                json.dump(chara_game_data, fp, ensure_ascii=False, indent=2)

        if CONFIG['races'] in (targets or []):
            cur.execute("""SELECT
                             ri.id, ri.race_id, race.thumbnail_id, race.course_set, smp.id AS program_id,
                             td1.[text] AS race_name, td2.[text] AS track_name,
                             rcs.distance, rcs.ground, rcs.inout,
                             ri.[date], ri.[time],
                             race.[group], race.grade, race.entry_num
                           FROM race_instance ri
                           LEFT JOIN single_mode_program smp ON ri.id = smp.race_instance_id
                           LEFT JOIN race ON ri.race_id = race.id
                           LEFT JOIN race_course_set rcs ON race.course_set = rcs.id
                           LEFT JOIN text_data td1 ON td1.[index] = ri.id AND td1.category = 28
                           LEFT JOIN text_data td2 ON td2.[index] = rcs.race_track_id AND td2.category = 31
                           WHERE (race.[group] = 1 OR race.[group] = 7)
                           ORDER BY ri.id;""")
            rows = cur.fetchall()
            # Build list of dicts using cursor.description to get column names
            colnames = [d[0] for d in cur.description]
            races_game_data = [dict(zip(colnames, row)) for row in rows]
            with open(CONFIG['races'], 'w', encoding='utf-8') as fp:
                json.dump(races_game_data, fp, ensure_ascii=False, indent=2)

    finally:
        conn.close()


def load_json(path: str) -> Any:
    """Load JSON from path and re-raise with helpful message on failure."""
    try:
        with open(path, 'r', encoding='utf-8') as fp:
            return json.load(fp)
    except FileNotFoundError:
        raise FileNotFoundError(f"Required JSON file not found: {path}. Ensure the file exists in the repository.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON file {path}: {e}")


# --- Load data files (these are required) ---
# Ensure JSON assets exist (create from DB when missing) before attempting to load
ensure_game_data_jsons_exist()

raw = load_json(CONFIG['umadump_data'])
factor_map: dict[str, Any] = load_json(CONFIG['factor'])
skills_map: dict[str, Any] = load_json(CONFIG['skills'])
chara_map: dict[str, Any] = load_json(CONFIG['chara'])
races_list: list[dict[str, Any]] = load_json(CONFIG['races'])
races_map: dict[Any, dict[str, Any]] = {str(x['program_id']): x for x in races_list}


# --- Single-point helpers for schema failures ---

def schema_key_error(path: str, hint: Optional[str] = None) -> KeyError:
    """Construct a KeyError with a helpful message explaining the schema problem.

    path: the dotted path or description of what failed (e.g. "entry.succession_chara_array[10].card_id")
    hint: optional further advice for the user about which JSON to update.
    """
    msg = f"Missing or invalid data at: {path}. The source JSONs appear incomplete or changed."
    if hint:
        msg += f" Suggestion: update or fix {hint}."
    else:
        msg += " Suggestion: verify your game_data JSON files match the expected schema."
    return KeyError(msg)


def require_path(obj: Any, *keys: str) -> Any:
    """Traverse a dict-like object using the given keys and raise a schema KeyError if any key is missing.

    Example: require_path(entry, 'race_result_list')
    """
    cur = obj
    traversed = []
    for k in keys:
        traversed.append(k)
        if isinstance(cur, dict):
            if k not in cur:
                raise schema_key_error('.'.join(traversed), hint='the source JSONs')
            cur = cur[k]
        else:
            # Unexpected non-dict while traversing
            raise schema_key_error('.'.join(traversed), hint='the source JSONs')
    return cur


def require_one(iterable: Iterable[Any], predicate, path_desc: str, hint: Optional[str] = None):
    """Return the first item matching predicate or raise a schema KeyError.
    path_desc is a human-readable description of where we looked (used in error message).
    """
    for x in iterable:
        if predicate(x):
            return x
    raise schema_key_error(path_desc, hint=hint or 'the source JSONs')


def require_map_lookup(mapping: dict[str, Any], key: Any, map_name: str) -> Any:
    """Lookup key in mapping; raise schema KeyError if missing.

    map_name is used in the error suggestion (e.g. 'game_data/factor.json').
    """
    k = str(key)
    if k not in mapping:
        raise schema_key_error(f"{map_name}[{k}]", hint=map_name)
    return mapping[k]


# --- Domain helpers ---

def spark_string_from_id(fid: int) -> str:
    """Build a human readable spark string from an id and raise if factor base missing."""
    fid_str = str(int(fid))
    star = fid_str[-1]

    base_key = fid_str[:-1] + '1'
    name = require_map_lookup(factor_map, base_key, CONFIG['factor'])
    return f"{name} ★{star}"


def skill_string_from_id(sid: int) -> str:
    """Build a human readable skill string from an id and raise if skill missing."""
    sid_str = str(int(sid))
    return require_map_lookup(skills_map, sid_str, CONFIG['skills'])


def resolve_spark_array_field(lst: Any) -> list[str]:
    if not isinstance(lst, list):
        raise schema_key_error('expected list of factor ids', hint='umadump_data.json')
    return [spark_string_from_id(x) for x in lst]


def parse_name_and_star(spark_str: str):
    name, _, star = spark_str.partition('★')
    return name.strip().lower(), int(star)


def classify_factors(factors: Any) -> dict[str, list[int]]:
    """Split a flat list of factor IDs into typed buckets.

    Input: factors - list of numeric-like factor ids
    Output: dict with keys: blue_sparks, pink_sparks, green_sparks, white_sparks
    """
    buckets = {"blue_sparks": [], "pink_sparks": [], "green_sparks": [], "white_sparks": []}
    if not isinstance(factors, list):
        raise schema_key_error('expected list of factor ids', hint='umadump_data.json')

    for fid in factors:
        try:
            s = str(int(fid))
        except Exception:
            raise schema_key_error(f"invalid factor id: {fid}", hint='umadump_data.json')
        l = len(s)
        if l == 3:
            buckets["blue_sparks"].append(int(fid))
        elif l == 4:
            buckets["pink_sparks"].append(int(fid))
        elif l == 7:
            buckets["white_sparks"].append(int(fid))
        elif l == 8:
            buckets["green_sparks"].append(int(fid))
        else:
            # unknown sizes: treat as white by default
            buckets["white_sparks"].append(int(fid))

    return buckets


def aggregate_factors(main_factors: Optional[list[int]], left_factors: Optional[list[int]] = None,
                      right_factors: Optional[list[int]] = None) -> list[int]:
    """Aggregate factor star values across main/left/right lists.

    - Sums star digits for identical factor bases (same id without last digit).
    - Preserves encounter order (main first, then left, then right) when returning aggregated ids.
    - Returns a flat list of aggregated factor ids (base + summed_star).
    """
    left_factors = left_factors or []
    right_factors = right_factors or []
    main_factors = main_factors or []

    sums: dict[str, int] = {}
    order: list[str] = []

    def add_list(lst: Iterable[int]):
        for fid in lst or []:
            try:
                s = str(int(fid))
            except Exception:
                raise schema_key_error(f"invalid factor id: {fid}", hint='umadump_data.json')
            base = s[:-1]
            star = int(s[-1])
            if base not in sums:
                sums[base] = 0
                order.append(base)
            sums[base] += star

    add_list(main_factors)
    add_list(left_factors)
    add_list(right_factors)

    out: list[int] = []
    for base in order:
        total = sums.get(base, 0)
        if total < 1:
            total = 1
        out_id = int(base + str(total))
        out.append(out_id)

    return out


def calculate_rating(parsed_entry: dict[str, Any]) -> float:
    WEIGHTS = {
        "total_sparks": 1.0,  # weight applied to total_spark_count
        "win": 0.5,  # weight per win
        "green_count_bonus": 3.0,  # bonus per additional green spark (count-based)
        "low_main_penalty_per_star": 2.0,  # penalty per missing star below threshold
        "main_threshold": 2,  # threshold star for main sparks (>=2 is OK)
        "distance_conflict_penalty": 5.0,
        "aptitude_conflict_penalty": 4.0,
        # parent_rank related thresholds / adjustments
        "parent_low_threshold": 8000,
        "parent_high_threshold": 10000,
        "parent_rank_low_penalty": -2.0,  # applied when parent_rank < low_threshold
        "parent_rank_high_bonus": 2.0,  # applied when parent_rank > high_threshold
        # penalty for missing main green spark
        "missing_green_penalty": 2.0
    }

    # keywords to detect conflicts in spark names
    DISTANCE_KEYWORDS = ['sprint', 'mile', 'medium', 'long']
    APTITUDE_KEYWORDS = ['front', 'pace', 'late', 'end']

    score = 0.0
    sparks_data = parsed_entry["sparks"]

    # base: total sparks (weighted)
    score += sparks_data["total_spark_count"] * WEIGHTS["total_sparks"]

    score -= (sparks_data["white_count"] - sparks_data[
        "main_white_count"]) * 0.5  # small penalty for non-main white sparks

    # bonus for having multiple green spark entries regardless of their star level
    num_green_entries = len(sparks_data["green_sparks"])
    if num_green_entries > 1:
        score += (num_green_entries - 1) * WEIGHTS["green_count_bonus"]

    # win count bonus
    score += parsed_entry["win_count"] * WEIGHTS["win"]

    # penalty for low-value main sparks (lower score for values below threshold)
    for key in ("main_blue_spark", "main_pink_spark"):
        s = sparks_data.get(key)
        if s is None:
            raise schema_key_error(f"expected {key} in sparks", hint='umadump_data.json')
        name, star = parse_name_and_star(s)
        if star < WEIGHTS["main_threshold"]:
            score -= (WEIGHTS["main_threshold"] - star) * WEIGHTS["low_main_penalty_per_star"]

    # penalty for missing main green spark
    if not sparks_data.get("main_green_spark"):
        score -= WEIGHTS["missing_green_penalty"]

    # detect conflicting distance types and aptitude types across all resolved spark names
    distance_types = set()
    aptitude_types = set()

    # collect all spark names (main + lists)
    all_sparks = sparks_data["blue_sparks"][:] + sparks_data["pink_sparks"][:]

    for sp in all_sparks:
        name, star = parse_name_and_star(sp)
        # distance detection
        if any(k in name for k in DISTANCE_KEYWORDS):
            # use the specific keyword found as a "type"
            for k in DISTANCE_KEYWORDS:
                if k in name:
                    distance_types.add(k)
        # aptitude detection
        if any(k in name for k in APTITUDE_KEYWORDS):
            for k in APTITUDE_KEYWORDS:
                if k in name:
                    aptitude_types.add(k)

    if len(distance_types) > 1:
        # penalize conflicting distance types
        score -= WEIGHTS["distance_conflict_penalty"] * (len(distance_types) - 1)

    if len(aptitude_types) > 1:
        # penalize conflicting aptitude types
        score -= WEIGHTS["aptitude_conflict_penalty"] * (len(aptitude_types) - 1)

    # parent_rank adjustment: penalize low parent_rank (< low_threshold), boost high parent_rank (> high_threshold)
    parent_rank = parsed_entry["parent_rank"]
    if parent_rank < WEIGHTS["parent_low_threshold"]:
        score += WEIGHTS["parent_rank_low_penalty"]
    elif parent_rank > WEIGHTS["parent_high_threshold"]:
        score += WEIGHTS["parent_rank_high_bonus"]

    return round(max(score, 0.0), 2)


# --- Main transformation ---

def make_cleaned() -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []

    for entry in raw:
        c: dict[str, Any] = {}

        def uma_name_only(uma_data: dict[str, Any], key: str) -> Optional[str]:
            # value may be explicitly null in source; ensure the key exists, then map
            if not isinstance(uma_data, dict):
                raise schema_key_error('uma_data is not a dict', hint='umadump_data.json')
            if key not in uma_data:
                raise schema_key_error(f"missing key {key} in uma_data", hint='umadump_data.json')
            val = uma_data[key]
            if val is None:
                return None
            # map lookup must exist
            return require_map_lookup(chara_map, str(val), '../game_data/chara.json')

        # helpers to get left/right parent entries; raise if missing
        succession = require_path(entry, 'succession_chara_array')
        left_parent = require_one(succession, lambda x: x.get('position_id') == 10,
                                  'entry.succession_chara_array position_id==10', hint='umadump_data.json')
        right_parent = require_one(succession, lambda x: x.get('position_id') == 20,
                                   'entry.succession_chara_array position_id==20', hint='umadump_data.json')

        c['parent_rank'] = require_path(entry, 'rank_score')
        c['parent_rarity'] = require_path(entry, 'rank')

        c['uma'] = {
            'main_parent': uma_name_only(entry, 'card_id'),
            'parent_left': uma_name_only(left_parent, 'card_id'),
            'parent_right': uma_name_only(right_parent, 'card_id')
        }

        c['stats'] = {
            'speed': require_path(entry, 'speed'),
            'stamina': require_path(entry, 'stamina'),
            'power': require_path(entry, 'power'),
            'guts': require_path(entry, 'guts'),
            'wisdom': require_path(entry, 'wiz')
        }

        c['fans'] = require_path(entry, 'fans')
        c['scenario_id'] = require_path(entry, 'scenario_id')

        def affinity_from_value(val: int) -> str:
            scale = ["Unknown", "G", "F", "E", "D", "C", "B", "A", "S"]
            try:
                return scale[val]
            except Exception:
                raise schema_key_error(f"invalid affinity value: {val}", hint='umadump_data.json')

        c['affinities'] = {
            'track': {
                'turf': affinity_from_value(require_path(entry, 'proper_ground_turf')),
                'dirt': affinity_from_value(require_path(entry, 'proper_ground_dirt'))
            },
            'distance': {
                'sprint': affinity_from_value(require_path(entry, 'proper_distance_short')),
                'mile': affinity_from_value(require_path(entry, 'proper_distance_mile')),
                'medium': affinity_from_value(require_path(entry, 'proper_distance_middle')),
                'long': affinity_from_value(require_path(entry, 'proper_distance_long'))
            },
            'style': {
                'front': affinity_from_value(require_path(entry, 'proper_running_style_nige')),
                'pace': affinity_from_value(require_path(entry, 'proper_running_style_senko')),
                'late': affinity_from_value(require_path(entry, 'proper_running_style_sashi')),
                'end': affinity_from_value(require_path(entry, 'proper_running_style_oikomi'))
            }
        }

        # skills: ensure skill_array exists and each item has skill_id
        skills_array = require_path(entry, 'skill_array')
        if not isinstance(skills_array, list):
            raise schema_key_error('skill_array is not a list', hint='umadump_data.json')
        c['skills'] = [skill_string_from_id(require_path(s, 'skill_id')) for s in skills_array]

        s: dict[str, Any] = {}

        all_factors = classify_factors(
                aggregate_factors(require_path(entry, 'factor_id_array'),
                                  require_path(left_parent, 'factor_id_array'),
                                  require_path(right_parent, 'factor_id_array')))
        main_factors = classify_factors(require_path(entry, 'factor_id_array'))

        s['blue_sparks'] = resolve_spark_array_field(all_factors['blue_sparks'])
        s['pink_sparks'] = resolve_spark_array_field(all_factors['pink_sparks'])
        s['green_sparks'] = resolve_spark_array_field(all_factors['green_sparks'])
        s['white_sparks'] = resolve_spark_array_field(all_factors['white_sparks'])

        def is_g1_win(race_entry: dict[str, Any]) -> bool:
            race_data = require_map_lookup(races_map, require_path(race_entry, 'program_id'), '../game_data/races.json')
            return require_path(race_entry, 'result_rank') == 1 and race_data['grade'] == 100 and race_data[
                'group'] == 1

        s['blue_count'] = sum(int(x[-1]) for x in s['blue_sparks'])
        s['pink_count'] = sum(int(x[-1]) for x in s['pink_sparks'])
        s['green_count'] = sum(int(x[-1]) for x in s['green_sparks'])
        s['white_count'] = len(s['white_sparks'])
        s['total_spark_count'] = s['blue_count'] + s['pink_count'] + s['green_count'] + s['white_count']

        # main sparks: require there is at least one value for main blue/pink
        mb = resolve_spark_array_field(main_factors['blue_sparks'])
        mp = resolve_spark_array_field(main_factors['pink_sparks'])
        if not mb or not mp:
            raise schema_key_error('expected main blue and main pink sparks in factor_id_array', hint='umadump_data.json')

        s['main_blue_spark'] = mb[0]
        s['main_pink_spark'] = mp[0]
        s['main_green_spark'] = next(iter(resolve_spark_array_field(main_factors['green_sparks'])), None)
        s['main_white_sparks'] = resolve_spark_array_field(main_factors['white_sparks'])
        s['main_white_count'] = len(s['main_white_sparks'])

        c['sparks'] = s

        race_results = require_path(entry, 'race_result_list')
        if not isinstance(race_results, list):
            raise schema_key_error('race_result_list is not a list', hint='umadump_data.json')
        c['win_count'] = len([x for x in race_results if is_g1_win(x)])

        c['rating'] = calculate_rating(c)

        cleaned.append(c)

    cleaned.sort(key=lambda x: (x['rating'], x['sparks']['total_spark_count'], x['parent_rank']), reverse=True)

    for idx, uma in enumerate(cleaned, start=1):
        uma['rating_idx'] = idx

    out_path = CONFIG['output']
    with open(out_path, 'w', encoding='utf-8') as fp:
        json.dump(cleaned, fp, indent=2, ensure_ascii=False)

    print(f"Created {out_path} with {len(cleaned)} entries.")
    return cleaned


if __name__ == '__main__':
    make_cleaned()

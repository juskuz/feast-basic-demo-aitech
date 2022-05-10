import argparse
from datetime import datetime, timedelta
import pathlib
import string
from typing import Dict, Optional, List
from pathlib import Path
import logging
import shutil
import pytz

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


ALPHABET = np.array(list(string.ascii_uppercase + string.digits))
PLAYER_ID_LEN = 3
TS_GENERATOR_RANGE = {
    "hist": lambda: {
        "start": datetime.today().replace(tzinfo=pytz.utc).date() - timedelta(days=11), 
        "end": datetime.fromordinal(datetime.today().replace(tzinfo=pytz.utc).date().toordinal()) - timedelta(hours=1),
    },
    "curr": lambda: {
        "start": datetime.today().date(), 
        "end": datetime.now(),
    },
}


def parse_config_from_args() -> Dict[str, int]:
    mode_help = ("defines mode of generator " 
                "(hist - generates 10 days data until start of current day, "
                "curr - generates data from start of current to now, it appends data to existed parquet files)")
    parser = argparse.ArgumentParser(description="Time series (hourly data) generator for feast demo")
    parser.add_argument("user_count", help="number of generated users", type=int)
    parser.add_argument("dest_dir", help="detination for directory for generated data", type=str)
    parser.add_argument("--mode", help=mode_help, type=str, choices=["hist", "curr"], default="hist")
    parser.add_argument("--seed", default=123, help="genartor seed", type=int, nargs='?')
    args = parser.parse_args()
    return vars(args)


def set_seed(seed: int) -> None:
    np.random.seed(seed)


def generate_player_ids(user_count: int) -> List[str]:
    if user_count > len(ALPHABET) ** PLAYER_ID_LEN:
        raise ValueError("Cannot create more players than unique player ids ")
    
    ids = set()
    while len(ids) < user_count:
        id_ = "".join(np.random.choice(ALPHABET, PLAYER_ID_LEN))
        ids.add(id_)
    return sorted(list(ids)) 

def sample_payments(df):
    first_id = df["player_id"].iloc[0]
    df_1 = df[df["player_id"] == first_id]
    df_2 = df[df["player_id"] == first_id].sample(frac=0.01).sort_index()
    first_payments = pd.concat([df_1, df_2]).sort_index().drop_duplicates()
    second_id = "ZA9"
    df_1 = df[df["player_id"] == second_id]
    df_2 = df[df["player_id"] == second_id].sample(frac=0.01).sort_index()
    second_payments = pd.concat([df_1, df_2]).sort_index().drop_duplicates()
    return pd.concat([first_payments, second_payments]).sort_index().drop_duplicates().reset_index()


def generate_timeseries(mode: str, player_ids: List[str]):
    ts_range = TS_GENERATOR_RANGE[mode]()
    logging.info(f"Generating synthetic data from {ts_range['start']} to {ts_range['end']}...")
    ts = pd.date_range(**ts_range, freq="1H").to_series(name="ts")
    stats = pd.DataFrame({"player_id": player_ids}).merge(ts, how="cross")
    payments = sample_payments(stats)
    stats = stats.sample(frac=0.75).sort_index()
    stats["win_loss_ratio"] = np.random.uniform(0, 1, size=len(stats))
    stats["games_played"] = np.round(np.random.pareto(2, size=len(stats)) * 100) + 1
    stats["time_in_game"] = np.random.uniform(1, 3600, size=len(stats))
    logging.info(f"{len(stats)} stats rows generated")

    payments["amount"] = np.round(np.random.uniform(10, 1000, size=len(payments)), 2)
    payments["transactions"] = np.round(np.random.uniform(1, 10, size=len(payments)), 0)
    logging.info(f"{len(payments)} payments rows generated")

    return stats, payments


def save_to_parquet(path: pathlib.Path, df: pd.DataFrame, table_name: str, apend_mode: bool):
    table = pa.Table.from_pandas(df)
    if apend_mode:
        logging.info(f"Appending player {table_name} to {path.absolute()}")
    else:
        logging.info(f"Saving player {table_name} to {path.absolute()}")
        if path.exists():
            shutil.rmtree(path)
    pq.write_to_dataset(table, root_path=path)


def save_data(dest_dir: str, stats: pd.DataFrame, payments: pd.DataFrame, apend_mode=False):
    dir = Path(dest_dir)
    if not dir.exists():
        dir.mkdir(parents=True)

    save_to_parquet(dir / "player_stats", stats, "stats", apend_mode)
    save_to_parquet(dir / "player_payments", payments, "payments", apend_mode)


def main():
    logging.basicConfig(level=logging.INFO)

    config = parse_config_from_args()
    set_seed(config["seed"])
    player_ids = generate_player_ids(config["user_count"])
    stats, payments = generate_timeseries(config["mode"], player_ids)
    save_data(config["dest_dir"], stats, payments, config["mode"] == "curr")
    

if __name__ == "__main__":
    main()
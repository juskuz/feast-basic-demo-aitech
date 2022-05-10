from datetime import datetime
from os import stat

from feast import FeatureStore
import pandas as pd


def main():
    PLAYER_ID1 = "0QG"
    PLAYER_ID2 = "ZA9"

    fs = FeatureStore("feature_repo")
    payments = pd.read_parquet("generated_data/player_payments")[["player_id", "ts"]]
    payments = payments[payments["player_id"].isin([PLAYER_ID1, PLAYER_ID2])]
    stats = pd.read_parquet("generated_data/player_stats")[["player_id", "ts"]]
    stats = stats[stats["player_id"].isin([PLAYER_ID1, PLAYER_ID2])]
    entity_df = pd.concat([payments, stats]).sort_index().drop_duplicates().reset_index()

    print()
    print("----------------------HIST DATA FRAME----------------------")
    print(entity_df)
    
    trainging_df = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            "payments:amount",
            "payments:transactions",
            "stats:win_loss_ratio",
            "stats:games_played",
            "stats:time_in_game",
        ]
    ).to_df()

    print()
    print("----------------------ONLINE DATA FRAME PLAYER1----------------------")
    print(trainging_df[trainging_df["player_id"] == PLAYER_ID1].reset_index().drop(columns=["level_0"]))

    print()
    print("----------------------ONLINE DATA FRAME PLAYER2----------------------")
    print(trainging_df[trainging_df["player_id"] == PLAYER_ID2].reset_index().drop(columns=["level_0"]))

    entity_rows = [{"player_id": PLAYER_ID1}, {"player_id": PLAYER_ID2}]
    online_df = fs.get_online_features(
        features=[
            "payments:amount",
            "payments:transactions",
            "stats:win_loss_ratio",
            "stats:games_played",
            "stats:time_in_game",
        ],
        entity_rows=entity_rows
    ).to_df()

    print()
    print("----------------------ONLINE DATA FRAME----------------------")
    print(online_df[["player_id", "amount", "transactions", "win_loss_ratio", "games_played", "time_in_game"]])


if __name__ == "__main__":
    main()

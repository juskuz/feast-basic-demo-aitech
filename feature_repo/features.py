from datetime import timedelta

from feast import Entity, Feature, FeatureView, FileSource, ValueType


payments_source = FileSource(
    path="/content/generated_data/player_payments",
    event_timestamp_column="ts",
)

stats_source = FileSource(
    path="/content/generated_data/player_stats",
    event_timestamp_column="ts",
)

driver = Entity(name="player_id", value_type=ValueType.STRING, description="player id",)

payments_fv = FeatureView(
    name="payments",
    entities=["player_id"],
    ttl=timedelta(hours=6),
    features=[
        Feature("amount", ValueType.FLOAT),
        Feature("transactions", ValueType.INT32),
    ],
    batch_source=payments_source
)

stats_fv = FeatureView(
    name="stats",
    entities=["player_id"],
    ttl=timedelta(hours=6),
    features=[
        Feature("win_loss_ratio", ValueType.FLOAT),
        Feature("games_played", ValueType.INT32),
        Feature("time_in_game", ValueType.FLOAT),
    ],
    batch_source=stats_source
)

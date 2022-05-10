import pandas as pd


def main():
    df = pd.read_parquet("generated_data/player_payments")
    print()
    print("----------------------PAYMENTS DF----------------------")
    print(df)
    print("Length of df:", len(df))
    print("Types:")
    print(df.dtypes)

    df = pd.read_parquet("generated_data/player_stats")
    print()
    print("----------------------STATS DF----------------------")
    print(df)
    print("Length of df:", len(df))
    print("Types:")
    print(df.dtypes)

if __name__ == "__main__":
    main()

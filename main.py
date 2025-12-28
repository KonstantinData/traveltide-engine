import typer
import pandas as pd
from pathlib import Path
from src.traveltide.etl import load_bronze_data, process_bronze_to_silver, load_silver_data
from src.traveltide.features import engineer_features
from src.traveltide.model import SegmentationPipeline
from src.traveltide.plots import plot_radar_chart, plot_perk_dashboard # <--- Updated Import

app = typer.Typer()

@app.command()
def run_pipeline():
    print("🚀 Starting TravelTide Medallion Pipeline...\n")

    # 1. ETL
    try:
        # Check if silver exists, else process bronze
        try:
            users_s, sessions_s, flights_s, hotels_s = load_silver_data()
            print("   ℹ️ Loaded existing Silver data.")
        except FileNotFoundError:
            u, s, f, h = load_bronze_data()
            users_s, sessions_s, flights_s, hotels_s = process_bronze_to_silver(u, s, f, h)
    except Exception as e:
        print(f"❌ Error in ETL: {e}")
        return

    # 2. Features
    print("\n⚙️ Processing Silver -> Gold (Feature Engineering)...")
    features_gold = engineer_features(users_s, sessions_s, flights_s, hotels_s)

    # 3. Model
    print("\n🧠 Training Model...")
    GOLD_DIR = Path("data/gold")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = SegmentationPipeline(n_clusters=3, use_pca=True)
    results_df = pipeline.fit_predict(features_gold)

    results_df.to_parquet(GOLD_DIR / "user_segments.parquet", index=False)
    results_df.to_csv(GOLD_DIR / "user_segments.csv", index=False)

    print("\n📊 Cluster Statistics:")
    print(pipeline.get_cluster_stats(results_df))

    # 4. Plots (All 5 Perks)
    print("\n🎨 Generating Visuals...")
    df_grouped = pipeline.get_cluster_stats(results_df)
    plot_radar_chart(df_grouped)
    plot_perk_dashboard(results_df) # <--- New Function

    print(f"\n🏆 Pipeline finished! Check the 'images/' folder.")

if __name__ == "__main__":
    app()
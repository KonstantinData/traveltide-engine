import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi
from pathlib import Path

# Config
GOLD_DIR = Path("data/gold")
IMG_DIR = Path("images")
IMG_DIR.mkdir(exist_ok=True)

def plot_radar_chart(df_grouped):
    """
    Creates a Radar Chart to compare cluster profiles (DNA).
    """
    print("   🎨 Generating Radar Chart...")

    # Select numeric features
    features = ['avg_clicks', 'avg_flight_fare', 'avg_hotel_price', 'nights', 'checked_bags']

    # Min-Max Normalization for comparison
    data = df_grouped[features].copy()
    for feature in features:
        min_val = data[feature].min()
        max_val = data[feature].max()
        if max_val - min_val > 0:
            data[feature] = (data[feature] - min_val) / (max_val - min_val)
        else:
            data[feature] = 0.5

    # Setup Plot
    categories = list(data.columns)
    N = len(categories)
    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]

    plt.figure(figsize=(10, 10))
    ax = plt.subplot(111, polar=True)
    plt.xticks(angles[:-1], categories, color='grey', size=10)
    ax.set_rlabel_position(0)
    plt.yticks([0.25, 0.5, 0.75], ["25%", "50%", "75%"], color="grey", size=7)
    plt.ylim(0, 1)

    colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99', '#99CCFF']
    # Dynamic labels based on index
    cluster_labels = [f"Cluster {i}" for i in data.index]

    for i, row in enumerate(data.itertuples(index=False)):
        values = list(row)
        values += values[:1]
        ax.plot(angles, values, linewidth=2, linestyle='solid', label=cluster_labels[i])
        ax.fill(angles, values, color=colors[i % len(colors)], alpha=0.2)

    plt.title('Cluster DNA: Behavioral Profiles', size=20, y=1.1)
    plt.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))

    plt.savefig(IMG_DIR / "radar_chart.png")
    print(f"   ✅ Radar chart saved.")

def plot_perk_dashboard(df):
    """
    Generates a 5-panel dashboard to validate ALL 5 Perks.
    """
    print("   🎨 Generating Perk Validation Dashboard (5 Metrics)...")
    sns.set_theme(style="whitegrid")

    # Mapping Hypothesis -> Data Metric
    perk_map = [
        ('avg_clicks', 'Exclusive Discounts', 'High clicks indicate interest but hesitation.'),
        ('nights', '1 Night Free', 'Longer stays benefit most from a free night.'),
        ('avg_hotel_price', 'Free Hotel Meal', 'High spenders appreciate on-site amenities.'),
        ('checked_bags', 'Free Checked Bag', 'Relevant only if bags are actually checked.'),
        ('avg_flight_fare', 'No Cancellation Fees', 'High ticket price = High risk aversion.')
    ]

    fig, axes = plt.subplots(1, 5, figsize=(20, 5), sharey=False)
    fig.suptitle('Hypothesis Testing: Matching Clusters to Perks', fontsize=16)

    colors = ['#FF9999', '#66B2FF', '#99FF99'] # Cluster 0, 1, 2 colors

    for i, (col, perk_name, desc) in enumerate(perk_map):
        sns.barplot(
            ax=axes[i],
            x='cluster_id',
            y=col,
            data=df,
            hue='cluster_id',
            palette=colors,
            legend=False
        )
        axes[i].set_title(perk_name, fontweight='bold')
        axes[i].set_ylabel(col)
        axes[i].set_xlabel("Cluster")

        # Add values on top of bars
        for container in axes[i].containers:
            axes[i].bar_label(container, fmt='%.1f')

    plt.tight_layout()
    plt.savefig(IMG_DIR / "perk_dashboard.png", dpi=300)
    print(f"   ✅ Perk dashboard saved to {IMG_DIR}")

if __name__ == "__main__":
    # Test Run
    try:
        df = pd.read_parquet(GOLD_DIR / "user_segments.parquet")
        # Calc means for radar
        metric_cols = ['avg_clicks', 'avg_flight_fare', 'avg_hotel_price', 'nights', 'checked_bags']
        df_grouped = df.groupby('cluster_id')[metric_cols].mean()

        plot_radar_chart(df_grouped)
        plot_perk_dashboard(df)
    except Exception as e:
        print(f"Error: {e}")
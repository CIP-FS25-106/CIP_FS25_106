"""
historical_data_analysis.py - Module for visualizing historical train data

This module handles loading, processing, and creating visualizations
of the historical train data to analyze delays across different stations,
train categories, time periods, and other dimensions.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os
from pathlib import Path
from typing import List, Dict, Optional, Tuple


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("visualize_historical.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Constants
DELAY_THRESHOLD = 2  # Minutes threshold for considering a train delayed


def get_project_root() -> Path:
    """
    Get the path to the project root directory.
    
    Returns:
        Path: Project root directory path
    """
    return Path(__file__).parent.parent.parent


def ensure_results_directory() -> Path:
    """
    Create a results directory if it doesn't exist.
    
    Returns:
        Path: Path to results directory
    """
    project_root = get_project_root()
    results_dir = project_root / "results"
    
    if not results_dir.exists():
        results_dir.mkdir(parents=True)
        logger.info(f"Created results directory at {results_dir}")
    
    return results_dir


def get_target_stations() -> List[str]:
    """
    Define target stations for analysis.
    
    Returns:
        List[str]: Names of stations to include
    """
    return ["Zürich HB", "Luzern", "Genève"]


def load_and_prepare_data(file_path: Path, stations: List[str]) -> pd.DataFrame:
    """
    Load and prepare the data for visualization.
    
    Args:
        file_path: Path to the CSV file
        stations: List of station names to filter for
        
    Returns:
        pd.DataFrame: Prepared DataFrame
    """
    try:
        logger.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path)
        
        # Filter for target stations
        df = df[df["station_name"].isin(stations)]
        logger.info(f"Filtered for stations: {stations}, {len(df)} records remaining")
        
        # Convert ride_day to datetime
        df["ride_day"] = pd.to_datetime(df["ride_day"], errors="coerce")
        logger.info(f"Date range: {df['ride_day'].min()} to {df['ride_day'].max()}")
        
        # Remove extreme negative delays
        df_filtered = df[(df["DELAY"] >= -500)]
        removed_count = len(df) - len(df_filtered)
        if removed_count > 0:
            logger.info(f"Removed {removed_count} records with extreme negative delays")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error loading or preparing data: {e}")
        raise


def create_overview_plot(df: pd.DataFrame, results_dir: Path) -> None:
    """
    Create an overview stripplot of delay distribution.
    
    Args:
        df: Prepared DataFrame
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating overview delay stripplot")
        plt.figure(figsize=(8, 3))
        sns.stripplot(data=df, x="DELAY", jitter=False, alpha=0.5)
        plt.title("Overview of DELAY")
        plt.xlabel("Delay [min]")
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "overview_delay_plot.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Overview plot saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating overview plot: {e}")


def create_category_delay_barplot(df: pd.DataFrame, results_dir: Path) -> None:
    """
    Create a barplot showing average delay per train category.
    
    Args:
        df: Prepared DataFrame
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating average delay per train category barplot")
        
        # Group and sort
        avg_by_category = df.groupby("train_category")["DELAY"].mean().reset_index()
        avg_by_category = avg_by_category.sort_values(by="DELAY", ascending=False)
        
        # Plot
        plt.figure(figsize=(8, 3))
        ax = sns.barplot(data=avg_by_category, x="train_category", y="DELAY", palette="colorblind")
        
        # Add value labels on top of bars
        for i, bar in enumerate(ax.patches):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, height + 0.1, f"{height:.2f}", 
                    ha='center', va='bottom', fontsize=9)
        
        # Finetune
        ax.set_ylim(0, 26)
        plt.title("Average Delay per Train Category")
        plt.xlabel("Train Category")
        plt.ylabel("Average Delay [min]")
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "category_delay_barplot.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Category delay barplot saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating category delay barplot: {e}")


def create_delay_category_barplot(df: pd.DataFrame, stations: List[str], results_dir: Path) -> None:
    """
    Create a horizontal barplot showing percentage of trains in each delay category per station.
    
    Args:
        df: Prepared DataFrame
        stations: List of station names to include
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating delay category percentage barplot")
        
        # Count number of trains in each delay category
        counts = df.groupby(["station_name", "DELAY_CAT"]).size().reset_index(name="count")
        
        # Calculate percentages
        totals = counts.groupby("station_name")["count"].sum().reset_index(name="total")
        counts = counts.merge(totals, on="station_name")
        counts["percentage"] = 100 * counts["count"] / counts["total"]
        
        # Define the categories order and colors for looping and mapping
        categories = [
            "On time", 
            "2 to 5minutes", 
            "5 to 15minutes", 
            "more than 15minutes", 
            "Cancelled"
        ]
        colors = {
            "On time": "#88CCEE",
            "2 to 5minutes": "#117733",
            "5 to 15minutes": "#DDCC77",
            "more than 15minutes": "#CC6677",
            "Cancelled": "#AA4499"
        }
        
        # Start the plotting
        fig, ax = plt.subplots(figsize=(4, 3))
        bottom = {station: 0 for station in stations}
        
        for cat in categories:
            subset = counts[counts["DELAY_CAT"] == cat]
            heights = []
            
            for station in stations:
                val = subset[subset["station_name"] == station]["percentage"]
                percent = val.values[0] if not val.empty else 0
                heights.append(percent)
            
            bars = ax.barh(stations, heights, left=[bottom[st] for st in stations], color=colors[cat], label=cat)
            
            # Add percentage labels inside bars
            for i, bar in enumerate(bars):
                if bar.get_width() > 5:  # Only add text if there's enough space
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_y() + bar.get_height() / 2,
                        f"{bar.get_width():.1f}%",
                        ha="center", va="center", color="white", fontsize=9
                    )
                bottom[stations[i]] += bar.get_width()
        
        # Finetune
        ax.set_title("Train Delay Categories per Station")
        ax.set_xlabel("Trains [%]")
        ax.set_ylabel("Station")
        ax.legend(fontsize='x-small', bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "delay_category_barplot.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Delay category barplot saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating delay category barplot: {e}")


def create_bubble_chart(df: pd.DataFrame, stations: List[str], results_dir: Path) -> None:
    """
    Create a bubble chart showing delay frequency vs severity for each station.
    
    Args:
        df: Prepared DataFrame
        stations: List of station names to include
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating delay frequency vs severity bubble chart")
        
        # Mean, total and sum of delayed trains more than DELAY_THRESHOLD minutes by station
        summary = df[df["station_name"].isin(stations)].groupby("station_name").agg(
            avg_delay=("DELAY", "mean"),
            total_trains=("DELAY", "count"),
            delayed_trains=("DELAY", lambda x: (x > DELAY_THRESHOLD).sum())
        ).reset_index()
        
        # Calculate percentage of delayed trains
        summary["pct_delayed"] = 100 * summary["delayed_trains"] / summary["total_trains"]
        
        # Plot
        fig, ax = plt.subplots(figsize=(4, 3))
        sns.set_palette("colorblind")
        ax.scatter(
            summary["pct_delayed"],
            summary["avg_delay"],
            s=summary["total_trains"] / 100,  
            alpha=0.6,
            color="steelblue",
            edgecolors="black"
        )
        
        # Add labels
        for i, row in summary.iterrows():
            ax.text(row["pct_delayed"] + 0.5, row["avg_delay"], row["station_name"], fontsize=10)
        
        # Finetune
        ax.set_xlim(10, 17)
        ax.set_ylim(0, 2)
        ax.set_title("Station Delay Analysis: Frequency vs Severity")
        ax.set_xlabel("Delayed Trains [%]")
        ax.set_ylabel("Average Delay [min]")
        ax.grid(True)
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "bubble_chart.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Bubble chart saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating bubble chart: {e}")


def create_weekday_heatmap(df: pd.DataFrame, results_dir: Path) -> None:
    """
    Create a heatmap showing percentage of delayed trains by station and day of week.
    
    Args:
        df: Prepared DataFrame
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating weekday delay heatmap")
        
        # Extract weekday name
        df["day_of_week"] = df["ride_day"].dt.day_name()
        
        # Order weekdays
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=weekday_order, ordered=True)
        
        # Filter for delayed > DELAY_THRESHOLD minutes
        df["is_delayed"] = df["DELAY"] > DELAY_THRESHOLD
        
        # Group by station and weekday
        heatmap_data = df.groupby(["station_name", "day_of_week"]).agg(
            total=("DELAY", "count"),
            delayed=("is_delayed", "sum")
        ).reset_index()
        
        heatmap_data["pct_delayed"] = 100 * heatmap_data["delayed"] / heatmap_data["total"]
        
        # Pivot for heatmap
        pivot = heatmap_data.pivot(index="station_name", columns="day_of_week", values="pct_delayed")
        
        # Plot heatmap
        plt.figure(figsize=(8, 3))
        sns.heatmap(pivot, annot=True, fmt=".1f", cmap="RdYlGn_r", vmin=0, vmax=20)
        plt.title(f"Percentage of Delayed Trains (>{DELAY_THRESHOLD}min) by Station and Day of Week")
        plt.xlabel("Day of Week")
        plt.ylabel("Station")
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "weekday_heatmap.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Weekday heatmap saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating weekday heatmap: {e}")


def create_hourly_lineplot(df: pd.DataFrame, results_dir: Path) -> None:
    """
    Create a line plot showing percentage of delayed trains by hour of the day for each station.
    
    Args:
        df: Prepared DataFrame
        results_dir: Directory to save the plot
    """
    try:
        logger.info("Creating hourly delay line plot")
        
        # Convert arrival planned column
        df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"], errors="coerce")
        
        # Extract hour of the day
        df["hour"] = df["scheduled_arrival"].dt.hour
        
        # Define what is considered a delay
        df["is_delayed"] = df["DELAY"] > DELAY_THRESHOLD
        
        # Group by hour and station
        delay_by_hour = df.groupby(["hour", "station_name"]).agg(
            total=("DELAY", "count"),
            delayed=("is_delayed", "sum")
        ).reset_index()
        
        # Calculate percentage
        delay_by_hour["pct_delayed"] = 100 * delay_by_hour["delayed"] / delay_by_hour["total"]
        
        # Line plotting
        plt.figure(figsize=(8, 2))
        sns.lineplot(data=delay_by_hour, x="hour", y="pct_delayed", 
                     hue="station_name", marker="o", palette="colorblind")
        plt.title(f"Percentage of Delayed Trains (>{DELAY_THRESHOLD} min) by Hour of the Day")
        plt.xlabel("Hour of the Day")
        plt.ylabel("Delayed Trains [%]")
        plt.xticks(range(0, 24))
        plt.grid(True)
        plt.legend(fontsize='small')
        plt.tight_layout()
        
        # Save figure
        output_path = results_dir / "hourly_lineplot.png"
        plt.savefig(output_path, dpi=300)
        logger.info(f"Hourly line plot saved to {output_path}")
        
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(f"Error creating hourly line plot: {e}")


def main():
    """Main function to execute the visualization process."""
    try:
        # Get project root path and ensure results directory exists
        project_root = get_project_root()
        results_dir = ensure_results_directory()
        
        # Define file path and target stations
        file_path = project_root / "data" / "historical" / "processed" / "historical_transformed.csv"
        stations = get_target_stations()        
        # Load and prepare data
        # df = load_and_prepare_data(file_path, stations)
        df = load_and_prepare_data(file_path, stations)
        
        # Create visualizations
        create_overview_plot(df, results_dir)
        create_category_delay_barplot(df, results_dir)
        create_delay_category_barplot(df, stations, results_dir)
        create_bubble_chart(df, stations, results_dir)
        create_weekday_heatmap(df, results_dir)
        create_hourly_lineplot(df, results_dir)
        
        logger.info(f"All visualizations completed successfully and saved to {results_dir}")
    
    except Exception as e:
        logger.error(f"Unexpected error in main process: {e}")


if __name__ == "__main__":
    main()
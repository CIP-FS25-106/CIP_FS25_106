import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# get the path to the project rootdir
project_root = Path(__file__).parent.parent

# load the dataframe
folder = project_root / "data" / "historical" / "processed" / "historical_transformed.csv"
df = pd.read_csv(folder)

# define again station (will be used also for looping through)
stations = ["Zürich HB", "Luzern", "Genève"]
df = df[df["station_name"].isin(stations)]

# convert ride_day to datetime
df["ride_day"] = pd.to_datetime(df["ride_day"], errors="coerce")

# print range of ride_date
print("Min ride_day:", df["ride_day"].min())
print("Max ride_day:", df["ride_day"].max())


###################################################################
# 1. Overview plot delay
###################################################################
plt.figure(figsize=(8, 3))
sns.stripplot(data=df, x="DELAY", jitter=False, alpha=0.5)
plt.title("Overview of DELAY")
plt.xlabel("Delay [min]")
plt.tight_layout()
plt.show()

# Remove extreme negative delays
df = df[(df["DELAY"] >= -500)]


###################################################################
# 2. Barplot: Average Delay per Train Category
###################################################################

# counting number of trains in each delay category
counts = df.groupby(["station_name", "DELAY_CAT"]).size().reset_index(name="count")

# calculate percentages
totals = counts.groupby("station_name")["count"].sum().reset_index(name="total")
counts = counts.merge(totals, on="station_name")
counts["percentage"] = 100 * counts["count"] / counts["total"]

# define the categories order and colors for looping and mapping
categories = ["On time", 
              "2 to 5minutes", 
              "5 to 15minutes", 
              "more than 15minutes", 
              "Cancelled"]
colors = {
    "On time": "#88CCEE",
    "2 to 5minutes": "#117733",
    "5 to 15minutes": "#DDCC77",
    "more than 15minutes": "#CC6677",
    "Cancelled": "#AA4499"
}

# start the plotting
fig, ax = plt.subplots(figsize=(8,3))
bottom = {station: 0 for station in stations}

for cat in categories:
    subset = counts[counts["DELAY_CAT"] == cat]
    heights = []

    for station in stations:
        val = subset[subset["station_name"] == station]["percentage"]
        percent = val.values[0] if not val.empty else 0
        heights.append(percent)

    bars = ax.barh(stations, heights, left=[bottom[st] for st in stations], color=colors[cat], label=cat)

    # add the percentage labels inside bars <-- had to look that up
    for i, bar in enumerate(bars):
        if bar.get_width() > 5: # <-- needed for resovling overlapping text issue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_y() + bar.get_height() / 2,
                f"{bar.get_width():.1f}%",
                ha="center", va="center", color="white", fontsize=9
            )
        bottom[stations[i]] += bar.get_width()

# finetune
ax.set_title("Train Delay Categories per Station")
ax.set_xlabel("Trains [%]")
ax.set_ylabel("Station")
ax.legend(title="Delay Category", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.show()


###################################################################
# 2. Barplot: Average Delay per Train Category
###################################################################
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

# finetune
ax.set_ylim(0, 26)
plt.title("Average Delay per Train Category")
plt.xlabel("Train Category")
plt.ylabel("Average Delay [min]")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


###################################################################
# 3. Bubble Chart: Delay Frequency vs Severity
###################################################################
# mean, total and sum of delayed train more than 2minuts BY station calulation
summary = df[df["station_name"].isin(stations)].groupby("station_name").agg(
    avg_delay=("DELAY", "mean"),
    total_trains=("DELAY", "count"),
    delayed_trains=("DELAY", lambda x: (x > 2).sum())
).reset_index()

# percentage of delayed trains calcultation
summary["pct_delayed"] = 100 * summary["delayed_trains"] / summary["total_trains"]

# plot
fig, ax = plt.subplots(figsize=(8, 3))
sns.set_palette("colorblind")
ax.scatter(
    summary["pct_delayed"],
    summary["avg_delay"],
    s=summary["total_trains"] / 100,  
    alpha=0.6,
    color="steelblue",
    edgecolors="black"
)

# add the labels
for i, row in summary.iterrows():
    ax.text(row["pct_delayed"] + 0.5, row["avg_delay"], row["station_name"], fontsize=10)

# finetune
ax.set_xlim(0, 17)
ax.set_ylim(0, 2)
ax.set_title("Station Delay Analysis: Frequency vs Severity")
ax.set_xlabel("Delayed Trains [%]")
ax.set_ylabel("Average Delay [min]")
ax.grid(True)
plt.tight_layout()#
plt.show()



###################################################################
# 4. Heat map: Delay weekday
###################################################################

# extract weekday name
df["day_of_week"] = df["ride_day"].dt.day_name()

# order weekday
weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=weekday_order, ordered=True)

# filter for delayed > 2min
df["is_delayed"] = df["DELAY"] > 2

# group_by station and weekday
heatmap_data = df.groupby(["station_name", "day_of_week"]).agg(
    total=("DELAY", "count"),
    delayed=("is_delayed", "sum")
).reset_index()

heatmap_data["pct_delayed"] = 100 * heatmap_data["delayed"] / heatmap_data["total"]

# pivot needed for heatmpa
pivot = heatmap_data.pivot(index="station_name", columns="day_of_week", values="pct_delayed")

# Plot heatmap
plt.figure(figsize=(8, 4))
sns.heatmap(pivot, annot=True, fmt=".1f", cmap="RdYlGn_r", vmin=0, vmax=20)
plt.title("Percentage of Delayed Trains (>2min) by Station and Day of Week")
plt.xlabel("Day of Week")
plt.ylabel("Station")
plt.tight_layout()
plt.show()


###################################################################
# 5. line plot: time 
###################################################################

# convert arrival planned col
df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"], errors="coerce")

# extract hour of the day
df["hour"] = df["scheduled_arrival"].dt.hour

# define what is considered a delay (e.g., >2 minutes)
df["is_delayed"] = df["DELAY"] > 2

# group_by by hour and station
delay_by_hour = df.groupby(["hour", "station_name"]).agg(
    total=("DELAY", "count"),
    delayed=("is_delayed", "sum")
).reset_index()

# cal percentage
delay_by_hour["pct_delayed"] = 100 * delay_by_hour["delayed"] / delay_by_hour["total"]

# lineplotting
plt.figure(figsize=(8, 3))
sns.lineplot(data=delay_by_hour, x="hour", y="pct_delayed", hue="station_name", marker="o", palette="colorblind")
plt.title("Percentage of Delayed Trains (>2 min) by Hour of the Day")
plt.xlabel("Hour of the Day")
plt.ylabel("Delayed Trains [%]")
plt.xticks(range(0, 24))
plt.grid(True)
plt.tight_layout()
plt.show()
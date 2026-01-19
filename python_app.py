import pandas as pd
import time

CSV_PATH = "crimes_1gb.csv"

print("=== SAMOSTALNA PYTHON APLIKACIJA (PANDAS) ===")
print(f"Korišćeni fajl: {CSV_PATH}")


def load_csv():
    return pd.read_csv(CSV_PATH, low_memory=False)


def run_filter_count(year, crime_type):
    print("\n----------------------------------")
    print(f"Zadatak: Broj krivičnih dela tipa '{crime_type}' u {year}. godini")

    start_total = time.time()

    df = load_csv()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")

    result = df[(df["Year"] == year) & (df["Primary Type"] == crime_type)]
    count = result.shape[0]

    print(f"Broj incidenata: {count}")
    print("Primer zapisa:")
    print(result.head(5).to_string(index=False))

    total_time = time.time() - start_total
    print(f"Ukupno vreme izvršenja: {round(total_time, 3)} sekundi")


def run_stats_by(group_col, numeric_col):
    print("\n==================================")
    print(f"Zadatak: Statistika za '{numeric_col}' grupisano po '{group_col}'")

    start_total = time.time()

    df = load_csv()
    df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

    stats = (
        df.groupby(group_col)[numeric_col]
          .agg(["min", "max", "mean", "std", "count"])
          .reset_index()
          .rename(columns={
              "min": "MIN",
              "max": "MAX",
              "mean": "AVG",
              "std": "STDDEV",
              "count": "BROJ"
          })
    )

    print("Rezultati (prvih 50 redova):")
    print(stats.head(50).to_string(index=False))

    total_time = time.time() - start_total
    print(f"Ukupno vreme izvršenja: {round(total_time, 3)} sekundi")


# ---------------- IZVRŠAVANJE TESTOVA ----------------

filter_tests = [
    (2020, "THEFT"),
    (2023, "BATTERY")
]

for year, crime in filter_tests:
    run_filter_count(year, crime)

stats_tests = [
    ("District", "Latitude"),
    ("District", "Longitude")
]

for group_col, num_col in stats_tests:
    run_stats_by(group_col, num_col)

print("\n==================================")
print("Sva testiranja su završena.")
print("==================================")

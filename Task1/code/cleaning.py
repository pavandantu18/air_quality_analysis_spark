import csv
import os

# ───── CONFIG ─────────────────────────────────────────────────────────────────
INPUT  = "/workspaces/air_quality_analysis_spark/Task1/output/combined_data_singlefile/part-00000-70caf08b-7aa9-4364-bf45-da7856bf320d-c000.csv"
OUTPUT = "/workspaces/air_quality_analysis_spark/Task1/output/output_task1.csv"

# the six pollutant columns, in order
PARAMS = ["pm1", "pm10", "pm25", "relativehumidity", "temperature", "um003"]

# ───── PROCESS ────────────────────────────────────────────────────────────────
if not os.path.isfile(INPUT):
    print(f"❌ Input file not found: {INPUT}")
    exit(1)

with open(INPUT, newline="") as fin, open(OUTPUT, "w", newline="") as fout:
    reader = csv.reader(fin)
    writer = csv.writer(fout)

    # write the single clean header
    header = ["datetime", "location", "lat", "lon"] + PARAMS
    writer.writerow(header)

    first_row = True
    for row in reader:
        # skip the input header if present
        if first_row and row and row[0].strip().lower() == "datetime":
            first_row = False
            continue
        first_row = False

        # re-join and strip unwanted chars, then split back out
        line       = ",".join(row).strip()
        clean_line = line.replace("[", "").replace("]", "").replace("'", "")
        parts      = clean_line.split(",")

        # drop the stray 'parameter' column if still present in position 4
        if len(parts) > 4 and parts[4].strip().lower() == "parameter":
            parts.pop(4)

        # first 4 columns always datetime,location,lat,lon
        base = parts[:4]
        # the rest are the six pollutant values
        vals = parts[4:]

        # pad/truncate to exactly 6 pollutant columns
        vals = (vals + [""] * 6)[:6]

        # replace any empty string with "N/A"
        vals = [v if v.strip() != "" else "N/A" for v in vals]

        # write cleaned row
        writer.writerow(base + vals)

print(f"✅ Clean CSV with N/A placeholders written to {OUTPUT}")

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

##############################
# PART A: CREATE A NEWS CSV (Using Actual Historical Dates)
##############################

core_events = [
    "CPI (YoY)",
    "Non-Farm Payrolls (NFP)",
    "Unemployment Rate",
    "Crude Oil Inventories",
    "ISM Manufacturing PMI",
    "ISM Non-Manufacturing PMI",
    "Durable Goods Orders MoM",
    "Initial Jobless Claims"
]

# Typical release times (approximate, can adjust as needed)
EVENT_RELEASE_TIME = {
    "CPI (YoY)":                  "08:30:00",
    "Non-Farm Payrolls (NFP)":    "08:30:00",
    "Unemployment Rate":          "08:30:00",  # same as NFP
    "Crude Oil Inventories":      "10:30:00",
    "ISM Manufacturing PMI":      "10:00:00",
    "ISM Non-Manufacturing PMI":  "10:00:00",
    "Durable Goods Orders MoM":   "08:30:00",
    "Initial Jobless Claims":     "08:30:00"
}

# Last 24 known release dates (oldest to newest) for each event:
REAL_DATES = {
    "CPI (YoY)": [
        "2021-02-10","2021-03-10","2021-04-13","2021-05-12","2021-06-10","2021-07-13",
        "2021-08-11","2021-09-14","2021-10-13","2021-11-10","2021-12-10","2022-01-12",
        "2022-02-10","2022-03-10","2022-04-12","2022-05-11","2022-06-10","2022-07-13",
        "2022-08-10","2022-09-13","2022-10-13","2022-11-10","2022-12-13","2023-01-12"
    ],
    "Non-Farm Payrolls (NFP)": [
        "2021-02-05","2021-03-05","2021-04-02","2021-05-07","2021-06-04","2021-07-02",
        "2021-08-06","2021-09-03","2021-10-08","2021-11-05","2021-12-03","2022-01-07",
        "2022-02-04","2022-03-04","2022-04-01","2022-05-06","2022-06-03","2022-07-08",
        "2022-08-05","2022-09-02","2022-10-07","2022-11-04","2022-12-02","2023-01-06"
    ],
    "Unemployment Rate": [
        "2021-02-05","2021-03-05","2021-04-02","2021-05-07","2021-06-04","2021-07-02",
        "2021-08-06","2021-09-03","2021-10-08","2021-11-05","2021-12-03","2022-01-07",
        "2022-02-04","2022-03-04","2022-04-01","2022-05-06","2022-06-03","2022-07-08",
        "2022-08-05","2022-09-02","2022-10-07","2022-11-04","2022-12-02","2023-01-06"
    ],
    "Crude Oil Inventories": [
        "2023-01-04","2022-12-29","2022-12-21","2022-12-14","2022-12-07","2022-11-30",
        "2022-11-23","2022-11-16","2022-11-09","2022-11-02","2022-10-26","2022-10-19",
        "2022-10-12","2022-10-05","2022-09-28","2022-09-21","2022-09-14","2022-09-08",
        "2022-08-31","2022-08-24","2022-08-17","2022-08-10","2022-08-03","2022-07-27"
    ],
    "ISM Manufacturing PMI": [
        "2023-01-03","2022-12-01","2022-11-01","2022-10-03","2022-09-01","2022-08-01",
        "2022-07-01","2022-06-01","2022-05-02","2022-04-01","2022-03-01","2022-02-01",
        "2022-01-04","2021-12-01","2021-11-01","2021-10-01","2021-09-01","2021-08-02",
        "2021-07-01","2021-06-01","2021-05-03","2021-04-01","2021-03-01","2021-02-01"
    ],
    "ISM Non-Manufacturing PMI": [
        "2023-01-05","2022-12-05","2022-11-03","2022-10-05","2022-09-06","2022-08-03",
        "2022-07-06","2022-06-03","2022-05-04","2022-04-05","2022-03-03","2022-02-03",
        "2022-01-06","2021-12-03","2021-11-03","2021-10-05","2021-09-03","2021-08-04",
        "2021-07-06","2021-06-03","2021-05-05","2021-04-05","2021-03-03","2021-02-03"
    ],
    "Durable Goods Orders MoM": [
        "2022-12-23","2022-11-23","2022-10-27","2022-09-27","2022-08-24","2022-07-27",
        "2022-06-27","2022-05-25","2022-04-26","2022-03-24","2022-02-25","2022-01-27",
        "2021-12-23","2021-11-24","2021-10-27","2021-09-27","2021-08-25","2021-07-27",
        "2021-06-24","2021-05-27","2021-04-26","2021-03-24","2021-02-25","2021-01-27"
    ],
    "Initial Jobless Claims": [
        "2023-01-05","2022-12-29","2022-12-22","2022-12-15","2022-12-08","2022-12-01",
        "2022-11-23","2022-11-17","2022-11-10","2022-11-03","2022-10-27","2022-10-20",
        "2022-10-13","2022-10-06","2022-09-29","2022-09-22","2022-09-15","2022-09-08",
        "2022-09-01","2022-08-25","2022-08-18","2022-08-11","2022-08-04","2022-07-28"
    ],
}

rows = []
for event_name in core_events:
    release_time_str = EVENT_RELEASE_TIME.get(event_name, "08:30:00")
    for date_str in REAL_DATES[event_name]:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # Parse the typical release time (HH:MM:SS)
        h, m, s = map(int, release_time_str.split(":"))
        event_datetime = date_obj.replace(hour=h, minute=m, second=s)
        rows.append({"event": event_name, "time": event_datetime})

df = pd.DataFrame(rows)
df.sort_values(by="time", inplace=True)
df.to_csv("us_news_directory.csv", index=False)
print(f"[Part A] Created 'us_news_directory.csv' with {len(df)} rows of actual release dates.\n")

##############################
# PART B: FINAL ANALYSIS
##############################

# 1. DOWNLOAD & SAVE EUR/USD DAILY DATA
ticker = "EURUSD=X"
start_date = "2020-01-01"
end_date   = "2024-12-31"
price_csv  = "eurusd_daily_data.csv"

# Download daily data from Yahoo Finance
eurusd_data = yf.download(ticker, start=start_date, end=end_date)
eurusd_data.reset_index(inplace=True)
eurusd_data.to_csv(price_csv, index=False)
print(f"[Part B] EUR/USD daily data saved as '{price_csv}'.")

# 2. LOAD PRICE DATA & CONVERT COLUMNS
price_data = pd.read_csv(price_csv, parse_dates=['Date'])
price_data.set_index('Date', inplace=True)
for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
    if col in price_data.columns:
        price_data[col] = pd.to_numeric(price_data[col], errors="coerce")
price_data.index = price_data.index.normalize()

# 3. LOAD THE (NEWLY CREATED) US NEWS DIRECTORY
news_csv = "us_news_directory.csv"
news_events = pd.read_csv(news_csv, parse_dates=['time'])

# 4. CALCULATE ABSOLUTE PIP MOVEMENTS, DAILY RANGE, REVERSALS, & SAME DAY REVERSAL
pip_size = 0.0001
results = []

# Process news events
for _, event in news_events.iterrows():
    event_datetime = event['time']
    event_date = event_datetime.date()
    event_timestamp = pd.Timestamp(event_date)
    # Find the previous trading day's close
    cutoff_ts = event_timestamp - timedelta(days=1)
    prev_days = price_data[price_data.index <= cutoff_ts]
    if prev_days.empty:
        continue
    price_before = prev_days.iloc[-1]['Close']
    # Check if event day is a trading day
    if event_timestamp not in price_data.index:
        continue
    day_data = price_data.loc[event_timestamp]
    price_after = day_data['Close']
    daily_range_pips = (day_data['High'] - day_data['Low']) / pip_size
    initial_move = price_after - price_before
    movement_pips = abs(initial_move / pip_size)
    
    # Next-day reversal
    next_day_ts = event_timestamp + timedelta(days=1)
    if next_day_ts in price_data.index:
        price_next_day = price_data.loc[next_day_ts, 'Close']
        if initial_move > 0 and price_next_day < price_before:
            reversal = True
        elif initial_move < 0 and price_next_day > price_before:
            reversal = True
        else:
            reversal = False
    else:
        reversal = None

    # Same-day reversal
    if initial_move > 0:
        same_day_reversal = (day_data['High'] - price_after) > (price_after - price_before)
    elif initial_move < 0:
        same_day_reversal = (price_after - day_data['Low']) > abs(price_after - price_before)
    else:
        same_day_reversal = False

    results.append({
        'event_date': event_date,
        'event': event['event'],
        'price_before': price_before,
        'price_after': price_after,
        'movement_pips': movement_pips,
        'daily_range_pips': daily_range_pips,
        'reversal': reversal,
        'same_day_reversal': same_day_reversal
    })

# 4.5 PROCESS NON-NEWS (CONTROL) DAYS
news_dates = set(news_events['time'].dt.date)
for current_date in price_data.index:
    if current_date.date() in news_dates:
        continue
    previous_data = price_data[price_data.index < current_date]
    if previous_data.empty:
        continue
    price_before = previous_data.iloc[-1]['Close']
    day_data = price_data.loc[current_date]
    price_after = day_data['Close']
    daily_range_pips = (day_data['High'] - day_data['Low']) / pip_size
    initial_move = price_after - price_before
    movement_pips = abs(initial_move / pip_size)
    
    next_date = current_date + timedelta(days=1)
    if next_date in price_data.index:
        price_next_day = price_data.loc[next_date, 'Close']
        if initial_move > 0 and price_next_day < price_before:
            reversal = True
        elif initial_move < 0 and price_next_day > price_before:
            reversal = True
        else:
            reversal = False
    else:
        reversal = None

    if initial_move > 0:
        same_day_reversal = (day_data['High'] - price_after) > (price_after - price_before)
    elif initial_move < 0:
        same_day_reversal = (price_after - day_data['Low']) > abs(price_after - price_before)
    else:
        same_day_reversal = False

    results.append({
        'event_date': current_date.date(),
        'event': "No News",
        'price_before': price_before,
        'price_after': price_after,
        'movement_pips': movement_pips,
        'daily_range_pips': daily_range_pips,
        'reversal': reversal,
        'same_day_reversal': same_day_reversal
    })

results_df = pd.DataFrame(results)

# 5. ANALYZE & EXPORT RESULTS
if not results_df.empty:
    avg_abs_pips = results_df['movement_pips'].mean()
    avg_daily_range = results_df['daily_range_pips'].mean()
    reversal_mask = results_df['reversal'].dropna()
    reversal_rate = reversal_mask.mean() * 100 if not reversal_mask.empty else 0

    print(f"\n[Analysis] Overall average pip movement (ABSOLUTE): {avg_abs_pips:.2f} pips")
    print(f"[Analysis] Overall average daily range: {avg_daily_range:.2f} pips")
    print(f"[Analysis] Overall next-day reversal rate: {reversal_rate:.2f}%")

    # Functions to compute reversal rates in percentage
    def same_day_rev_rate_func(x):
        x = x.dropna()
        return x.mean() * 100 if not x.empty else 0

    def reversal_rate_func(x):
        x = x.dropna()
        return x.mean() * 100 if not x.empty else 0

    grouped = results_df.groupby('event').agg(
        avg_abs_pips=('movement_pips', 'mean'),
        avg_volatility_daily_range=('daily_range_pips', 'mean'),
        next_day_reversal_rate=('reversal', reversal_rate_func),
        same_day_reversal_rate=('same_day_reversal', same_day_rev_rate_func),
        sample_size=('movement_pips', 'count')
    )
    
    grouped['next_day_reversal_percentage'] = grouped['next_day_reversal_rate'].map("{:.2f}%".format)
    grouped['same_day_reversal_percentage'] = grouped['same_day_reversal_rate'].map("{:.2f}%".format)

    print("\n--- Grouped Stats by Event ---")
    print(grouped)

    # 6. EXPORT TO EXCEL + INSERT CHARTS, SUMMARY TABLE, & TEXT BOX
    excel_file = "news_analysis_results.xlsx"
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        results_df.to_excel(writer, sheet_name="Event Details", index=False)
        grouped.to_excel(writer, sheet_name="Grouped Stats")
        workbook  = writer.book
        worksheet = writer.sheets["Grouped Stats"]

        # 6.1) CHART 1: COLUMN CHART FOR Avg Pip Movement
        chart1 = workbook.add_chart({'type': 'column'})
        chart1.add_series({
            'name':       'Average Pip Movement',
            'categories': ['Grouped Stats', 1, 0, len(grouped.index), 0],
            'values':     ['Grouped Stats', 1, 1, len(grouped.index), 1],
        })
        chart1.set_title({'name': 'EUR/USD Price Movement by News Event'})
        chart1.set_x_axis({'name': 'Event'})
        chart1.set_y_axis({'name': 'Average Pip Movement (pips)'})
        worksheet.insert_chart('J2', chart1)

        # 6.2) CHART 2: BAR CHART FOR Avg Daily Range
        chart2 = workbook.add_chart({'type': 'bar'})
        chart2.add_series({
            'name':       'Average Daily Range',
            'categories': ['Grouped Stats', 1, 0, len(grouped.index), 0],
            'values':     ['Grouped Stats', 1, 2, len(grouped.index), 2],
        })
        chart2.set_title({'name': 'Daily Range by News Event'})
        chart2.set_x_axis({'name': 'Event'})
        chart2.set_y_axis({'name': 'Average Daily Range (pips)'})
        worksheet.insert_chart('J25', chart2)

        # 6.3) CHART 3: CLUSTERED COLUMN CHART FOR NEXT-DAY & SAME-DAY REVERSAL RATES
        chart3 = workbook.add_chart({'type': 'column', 'subtype': 'clustered'})
        chart3.add_series({
            'name':       'Next-Day Reversal Rate',
            'categories': ['Grouped Stats', 1, 0, len(grouped.index), 0],
            'values':     ['Grouped Stats', 1, 3, len(grouped.index), 3],
        })
        chart3.add_series({
            'name':       'Same-Day Reversal Rate',
            'categories': ['Grouped Stats', 1, 0, len(grouped.index), 0],
            'values':     ['Grouped Stats', 1, 4, len(grouped.index), 4],
        })
        chart3.set_title({'name': 'Reversal Rates by News Event'})
        chart3.set_x_axis({'name': 'Event'})
        chart3.set_y_axis({'name': 'Reversal Rate (%)'})
        worksheet.insert_chart('J45', chart3)

        # 6.4) SUMMARY TABLE
        summary_start_row = 60  # Adjust as needed
        summary_start_col = 9   # Column J
        worksheet.write(summary_start_row, summary_start_col,     "News Event")
        worksheet.write(summary_start_row, summary_start_col + 1, "Avg Pip Movement")
        worksheet.write(summary_start_row, summary_start_col + 2, "Avg Daily Range")
        worksheet.write(summary_start_row, summary_start_col + 3, "Next-Day Rev Rate")
        worksheet.write(summary_start_row, summary_start_col + 4, "Same-Day Rev Rate")
        
        for i, (event, row_data) in enumerate(grouped.iterrows(), start=summary_start_row+1):
            worksheet.write(i, summary_start_col,     event)
            worksheet.write(i, summary_start_col + 1, row_data['avg_abs_pips'])
            worksheet.write(i, summary_start_col + 2, row_data['avg_volatility_daily_range'])
            worksheet.write(i, summary_start_col + 3, row_data['next_day_reversal_percentage'])
            worksheet.write(i, summary_start_col + 4, row_data['same_day_reversal_percentage'])

        # 6.5) SINGLE TEXT BOX WITH ALL EXPLANATIONS
        text_box_message = (
            "CPI has a higher reversal rate, suggesting that even though the initial reaction is significant,\n"
            "the move doesn't sustain into the next day—leading to a lower net movement when you look at the close.\n"
            "In that sense, the lower average ABS pip movement for CPI could be related to its higher reversal rate.\n\n"
            "Average ABS pips tells you the average size of the immediate move on an event day (ignoring direction),\n"
            "while the reversal rate tells you how frequently that move gets undone the next day.\n\n"
            "If CPI shows a 62% next-day reversal rate, it means that on 62% of CPI days, the price move above\n"
            "the previous day's close is undone by the following day's close.\n\n"
            "If CPI shows a 62% same-day reversal rate, it means that 62% of the time, after moving above\n"
            "the previous day's close, the price retraces intraday and closes below that previous day's close."
        )
        worksheet.insert_textbox('R60', text_box_message, {
            'width': 600,
            'height': 200,
            'font': {'size': 11, 'color': 'black'},
            'align': {'vertical': 'top', 'horizontal': 'left'}
        })

    print(f"\n[Analysis] Results, charts, summary table, and combined text box exported to '{excel_file}'.")
else:
    print("[Analysis] No valid events processed. Please check your data.")

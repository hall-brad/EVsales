"""
Automated pipeline to update EV dashboard with latest data
Fetches CSV, performs analysis, and generates web data file
"""

import pandas as pd
import json
from datetime import datetime

print(f"Starting dashboard update at {datetime.now()}")

# Step 1: Read the CSV file
print("Step 1: Reading CSV data...")
df = pd.read_csv('all_carsales_monthly.csv')

# Extract year and month from YYYYMM
df['Year'] = df['YYYYMM'] // 100
df['Month'] = df['YYYYMM'] % 100

print(f"  - Loaded {len(df)} records")
print(f"  - Years covered: {df['Year'].min()} to {df['Year'].max()}")

# Step 2: Check data completeness
print("Step 2: Checking data completeness...")
completeness = df.groupby(['Country', 'Year'])['Month'].nunique().reset_index()
completeness.columns = ['Country', 'Year', 'Months_Available']
completeness['Is_Complete'] = completeness['Months_Available'] == 12

# Step 3: Group by Country, Year, and Fuel type
print("Step 3: Aggregating annual data...")
annual_data = df.groupby(['Country', 'Year', 'Fuel'])['Value'].sum().reset_index()

# Pivot to get fuel types as columns
pivot_data = annual_data.pivot_table(index=['Country', 'Year'], columns='Fuel', values='Value', fill_value=0).reset_index()

# Calculate EV sales (BatteryElectric + PluginHybrid)
pivot_data['EV_Sales'] = pivot_data.get('BatteryElectric', 0) + pivot_data.get('PluginHybrid', 0)

# Calculate total car sales from all fuel types
fuel_columns = [col for col in pivot_data.columns if col not in ['Country', 'Year', 'EV_Sales']]
pivot_data['Total_Sales'] = pivot_data[fuel_columns].sum(axis=1)

# Calculate EV percentage of total sales
pivot_data['EV_Percentage'] = (pivot_data['EV_Sales'] / pivot_data['Total_Sales'] * 100).round(2)

# Merge completeness data
pivot_data = pivot_data.merge(completeness, on=['Country', 'Year'], how='left')

# Calculate YoY growth for all years
pivot_data = pivot_data.sort_values(['Country', 'Year'])
pivot_data['Previous_Year_EV_Sales'] = pivot_data.groupby('Country')['EV_Sales'].shift(1)
pivot_data['YoY_Growth'] = ((pivot_data['EV_Sales'] - pivot_data['Previous_Year_EV_Sales']) / pivot_data['Previous_Year_EV_Sales'] * 100).round(2)

print(f"  - Processed {len(pivot_data)} country-year combinations")
print(f"  - Countries analyzed: {pivot_data['Country'].nunique()}")

# Step 4: Calculate global rankings for most recent year
print("Step 4: Calculating global rankings...")
latest_year = pivot_data['Year'].max()
df_latest = pivot_data[pivot_data['Year'] == latest_year].copy()
global_ev_total = df_latest['EV_Sales'].sum()

df_latest_sorted = df_latest.sort_values('EV_Sales', ascending=False).reset_index(drop=True)
df_latest_sorted['Rank'] = df_latest_sorted.index + 1
df_latest_sorted['Global_Share'] = (df_latest_sorted['EV_Sales'] / global_ev_total * 100)

rankings = {}
for _, row in df_latest_sorted.iterrows():
    rankings[row['Country']] = {
        'rank': int(row['Rank']),
        'global_share': float(row['Global_Share'])
    }

print(f"  - Rankings calculated for {latest_year}")
print(f"  - Global EV sales: {global_ev_total:,.0f}")

# Step 5: Build monthly EV sales data
print("Step 5: Building monthly breakdown data...")
ev_fuel_types = ['BatteryElectric', 'PluginHybrid']
monthly_ev = df[df['Fuel'].isin(ev_fuel_types)].groupby(['Country', 'Year', 'Month'])['Value'].sum().reset_index()

# Build nested dict: { country: { year: { month: ev_sales } } }
monthly_by_country = {}
for _, row in monthly_ev.iterrows():
    c_name = row['Country']
    y = int(row['Year'])
    m = int(row['Month'])
    v = int(row['Value'])
    if c_name not in monthly_by_country:
        monthly_by_country[c_name] = {}
    if y not in monthly_by_country[c_name]:
        monthly_by_country[c_name][y] = {}
    monthly_by_country[c_name][y][m] = v

print(f"  - Monthly data built for {len(monthly_by_country)} countries")

# Step 6: Convert to JSON format for JavaScript
print("Step 6: Generating web data file...")
countries_data = {}

for country in pivot_data['Country'].unique():
    country_df = pivot_data[pivot_data['Country'] == country].sort_values('Year')

    # Build monthly_ev_sales as { "2025": [jan, feb, ...], "2024": [...] }
    monthly_sales = {}
    if country in monthly_by_country:
        for yr in monthly_by_country[country]:
            months_dict = monthly_by_country[country][yr]
            monthly_arr = [months_dict.get(m, None) for m in range(1, 13)]
            monthly_sales[str(yr)] = monthly_arr

    countries_data[country] = {
        'years': country_df['Year'].tolist(),
        'ev_sales': country_df['EV_Sales'].tolist(),
        'total_sales': country_df['Total_Sales'].tolist(),
        'ev_percentage': country_df['EV_Percentage'].tolist(),
        'yoy_growth': [],
        'months_available': country_df['Months_Available'].tolist(),
        'is_complete': country_df['Is_Complete'].apply(lambda x: 'Yes' if x else 'No').tolist(),
        'monthly_ev_sales': monthly_sales,
        'rank': None,
        'global_share': None
    }

    # Get YoY growth for all years (stored as percentage)
    for _, row_data in country_df.iterrows():
        growth = row_data['YoY_Growth']
        if pd.notna(growth):
            countries_data[country]['yoy_growth'].append(float(growth))
        else:
            countries_data[country]['yoy_growth'].append(None)

    # Add ranking and global share
    if country in rankings:
        countries_data[country]['rank'] = rankings[country]['rank']
        countries_data[country]['global_share'] = rankings[country]['global_share']

# Write JSON data
with open('ev_data.js', 'w') as f:
    f.write('const evData = ')
    json.dump(countries_data, f, indent=2)
    f.write(';')

print(f"Step 7: Complete!")
print(f"  - Updated ev_data.js with {len(countries_data)} countries")
print(f"  - Data range: {pivot_data['Year'].min()} to {pivot_data['Year'].max()}")
print(f"\nDashboard update finished at {datetime.now()}")

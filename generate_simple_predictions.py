import pandas as pd
import numpy as np
from datetime import datetime
from simple_crime_prediction_model import model, scaler, features
from sklearn.impute import SimpleImputer

# Column mapping from original to renamed
feature_mapping = {
    'POPESTIMATE': 'Total_Population',
    'PerCapitaInc': 'Per_Capita_Income',
    'PovertyAllAgesPct': 'Poverty_Rate',
    'Ed1LessThanHSPct': 'Less_Than_HS_Education',
    'Ed3SomeCollegePct': 'Some_College_Education',
    'Ed5CollegePlusPct': 'College_Plus_Education',
    'OwnHomePct': 'Home_Ownership_Rate',
    'NumUnemployed': 'Unemployment_Rate',
    'Housing_Units': 'Housing_Units',
    'Diversity-Index': 'Diversity_Index',
    'MEDIAN_AGE_TOT': 'Median_Age'
}

# Load the original data
print("Loading original data...")
data = pd.read_csv('US_data/detailed_county_wise_crime_data.csv')

# Calculate medians for imputation
medians = {
    new_name: data[old_name].median()
    for old_name, new_name in feature_mapping.items()
}

# Create date range for predictions (2024-2025)
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)
dates = pd.date_range(start=start_date, end=end_date, freq='M')

# Initialize list to store predictions
predictions = []

print("Generating predictions for each county...")
# Get unique FIPS codes
counties = data['FIPS'].unique()

for fips in counties:
    # Get the most recent data for this county
    county_data = data[data['FIPS'] == fips].iloc[-1]
    
    # Prepare input data using original column names
    input_dict = {}
    for old_name, new_name in feature_mapping.items():
        value = county_data[old_name]
        # Use median if value is missing
        if pd.isna(value):
            value = medians[new_name]
        input_dict[new_name] = value
    
    input_data = pd.DataFrame([input_dict])
    
    # Get base prediction
    base_prediction = model.predict(scaler.transform(input_data[features]))[0]
    
    # Generate monthly predictions with small variations
    for date in dates:
        # Add random variation (±5%) to make monthly predictions more realistic
        variation = np.random.uniform(0.95, 1.05)
        monthly_prediction = base_prediction * variation
        
        # Store prediction with metadata
        predictions.append({
            'Date': date,
            'FIPS': fips,
            'State_FIPS': str(fips)[:2],  # First 2 digits of FIPS is state code
            'County_FIPS': str(fips)[2:],  # Last 3 digits of FIPS is county code
            'Predicted_Crime_Rate': monthly_prediction,
            'Base_Prediction': base_prediction,
            'Total_Population': county_data['POPESTIMATE'],
            'Per_Capita_Income': county_data['PerCapitaInc'],
            'Poverty_Rate': county_data['PovertyAllAgesPct'],
            'Unemployment_Rate': county_data['NumUnemployed'],
            'Diversity_Index': county_data['Diversity-Index'],
            'Median_Age': county_data['MEDIAN_AGE_TOT']
        })

# Create DataFrame from predictions
print("Creating prediction dataset...")
predictions_df = pd.DataFrame(predictions)

# Add columns useful for Tableau
predictions_df['Year'] = predictions_df['Date'].dt.year
predictions_df['Month'] = predictions_df['Date'].dt.month
predictions_df['Quarter'] = predictions_df['Date'].dt.quarter
predictions_df['Month_Name'] = predictions_df['Date'].dt.strftime('%B')
predictions_df['Predicted_Crimes'] = (predictions_df['Predicted_Crime_Rate'] * predictions_df['Total_Population'] / 100000).round()

# Save predictions to CSV
output_file = 'crime_predictions_for_tableau.csv'
predictions_df.to_csv(output_file, index=False)
print(f"\nPredictions saved to {output_file}")

# Print summary statistics
print("\nPrediction Summary:")
print(f"Number of counties: {len(counties)}")
print(f"Date range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
print(f"Total records generated: {len(predictions_df)}")

# Print some sample predictions
print("\nSample Predictions:")
print(predictions_df[['Date', 'State_FIPS', 'Predicted_Crime_Rate', 'Predicted_Crimes']].head())

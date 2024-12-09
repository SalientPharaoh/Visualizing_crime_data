import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from crime_prediction_model import predict_violent_crime

# Load the original data
print("Loading original data...")
data = pd.read_csv('US_data/detailed_county_wise_crime_data.csv')

# Rename columns to match prediction model
column_rename_map = {
    'POPESTIMATE': 'Total_Population',
    'POPEST_MALE': 'Male_Population',
    'POPEST_FEM': 'Female_Population',
    'MEDIAN_AGE_TOT': 'Median_Age_Total',
    'MEDIAN_AGE_MALE': 'Median_Age_Male',
    'MEDIAN_AGE_FEM': 'Median_Age_Female',
    'PerCapitaInc': 'Per_Capita_Income',
    'PovertyAllAgesPct': 'Poverty_Rate',
    'Immigrant_Rate_2000_2010': 'Immigration_Rate',
    'Ed1LessThanHSPct': 'Less_Than_HS_Education',
    'Ed3SomeCollegePct': 'Some_College_Education',
    'Ed5CollegePlusPct': 'College_Plus_Education',
    'OwnHomePct': 'Home_Ownership_Rate',
    'NumUnemployed': 'Unemployment_Rate',
    'Housing_Units': 'Available_Housing_Units',
    'Diversity-Index': 'Diversity_Index',
    'Black or African American alone percentage': 'African_American_Pct',
    'Hispanic or Latino percentage': 'Hispanic_Latino_Pct',
    'Violent crime': 'Violent_Crime_Rate'
}

data = data.rename(columns=column_rename_map)

# Get unique FIPS codes (county identifiers)
counties = data['FIPS'].unique()

# Create date range for predictions (2024-2025)
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)
dates = pd.date_range(start=start_date, end=end_date, freq='M')

# Initialize lists to store prediction data
prediction_data = []

print("Generating predictions for each county...")
for fips in counties:
    county_data = data[data['FIPS'] == fips].iloc[-1]  # Get the most recent data for the county
    
    # Transform features
    total_pop_log = np.log1p(county_data['Total_Population'])
    per_capita_inc_log = np.log1p(county_data['Per_Capita_Income'])
    
    # Create derived features
    poverty_education = county_data['Poverty_Rate'] * county_data['Less_Than_HS_Education']
    income_education = per_capita_inc_log * county_data['College_Plus_Education']
    employment_education = county_data['Unemployment_Rate'] * county_data['College_Plus_Education']
    population_density = county_data['Total_Population'] / county_data['Available_Housing_Units']
    crime_risk_index = (county_data['Poverty_Rate'] * county_data['Unemployment_Rate'] * 
                       (100 - county_data['College_Plus_Education'])) / 1000
    education_index = (county_data['College_Plus_Education'] * 3 + 
                      county_data['Some_College_Education'] * 2 - 
                      county_data['Less_Than_HS_Education']) / 6
    economic_stress = (county_data['Poverty_Rate'] + county_data['Unemployment_Rate']) / 2
    social_stability = (county_data['Home_Ownership_Rate'] - county_data['Unemployment_Rate'] + 
                       county_data['College_Plus_Education']) / 3
    
    # Prepare input data for prediction with all required features
    input_data = {
        'Total_Population': total_pop_log,
        'Per_Capita_Income': per_capita_inc_log,
        'Poverty_Rate': county_data['Poverty_Rate'],
        'Less_Than_HS_Education': county_data['Less_Than_HS_Education'],
        'Some_College_Education': county_data['Some_College_Education'],
        'College_Plus_Education': county_data['College_Plus_Education'],
        'Home_Ownership_Rate': county_data['Home_Ownership_Rate'],
        'Unemployment_Rate': county_data['Unemployment_Rate'],
        'Available_Housing_Units': county_data['Available_Housing_Units'],
        'Diversity_Index': county_data['Diversity_Index'],
        'African_American_Pct': county_data['African_American_Pct'],
        'Hispanic_Latino_Pct': county_data['Hispanic_Latino_Pct'],
        'Median_Age_Total': county_data['Median_Age_Total'],
        'Poverty_Education': poverty_education,
        'Income_Education': income_education,
        'Employment_Education': employment_education,
        'Population_Density': population_density,
        'Crime_Risk_Index': crime_risk_index,
        'Education_Index': education_index,
        'Economic_Stress': economic_stress,
        'Social_Stability': social_stability
    }
    
    # Get prediction
    predicted_crime_rate = predict_violent_crime(input_data)
    
    # Create monthly variations (adding some randomness to make it more realistic)
    for date in dates:
        # Add small random variation (±5%) to the base prediction
        variation = np.random.uniform(0.95, 1.05)
        monthly_prediction = predicted_crime_rate * variation
        
        prediction_data.append({
            'Date': date,
            'FIPS': fips,
            'State': county_data['State'],
            'Predicted_Crime_Rate': monthly_prediction,
            'Base_Prediction': predicted_crime_rate,
            'Total_Population': county_data['Total_Population'],
            'Male_Population': county_data['Male_Population'],
            'Female_Population': county_data['Female_Population'],
            'Per_Capita_Income': county_data['Per_Capita_Income'],
            'Poverty_Rate': county_data['Poverty_Rate'],
            'Diversity_Index': county_data['Diversity_Index'],
            'African_American_Pct': county_data['African_American_Pct'],
            'Hispanic_Latino_Pct': county_data['Hispanic_Latino_Pct'],
            'Median_Age_Total': county_data['Median_Age_Total'],
            'Crime_Risk_Index': crime_risk_index,
            'Population_Density': population_density,
            'Economic_Stress': economic_stress,
            'Social_Stability': social_stability,
            'Available_Housing_Units': county_data['Available_Housing_Units']
        })

# Create DataFrame and save to CSV
print("Creating prediction dataset...")
predictions_df = pd.DataFrame(prediction_data)

# Add some derived columns useful for Tableau
predictions_df['Year'] = predictions_df['Date'].dt.year
predictions_df['Month'] = predictions_df['Date'].dt.month
predictions_df['Quarter'] = predictions_df['Date'].dt.quarter
predictions_df['Month_Name'] = predictions_df['Date'].dt.strftime('%B')
predictions_df['Predicted_Crimes'] = (predictions_df['Predicted_Crime_Rate'] * predictions_df['Total_Population'] / 100000).round()

# Save the predictions
output_file = 'crime_predictions_for_tableau.csv'
predictions_df.to_csv(output_file, index=False)
print(f"Predictions saved to {output_file}")

# Print some basic statistics
print("\nPrediction Summary:")
print(f"Number of counties: {len(counties)}")
print(f"Date range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
print(f"Total records generated: {len(predictions_df)}")

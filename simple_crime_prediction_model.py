import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

def train_model():
    """Train the Random Forest model on crime data"""
    print("Loading and preparing data...")
    data = pd.read_csv('US_data/detailed_county_wise_crime_data.csv')
    
    # Select and rename relevant features
    feature_columns = {
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
    
    # Rename columns
    data = data.rename(columns=feature_columns)
    
    # Select features for model
    features = [
        'Total_Population',
        'Per_Capita_Income',
        'Poverty_Rate',
        'Less_Than_HS_Education',
        'Some_College_Education',
        'College_Plus_Education',
        'Home_Ownership_Rate',
        'Unemployment_Rate',
        'Housing_Units',
        'Diversity_Index',
        'Median_Age'
    ]
    
    # Handle missing values
    data[features] = data[features].fillna(data[features].median())
    
    # Create target variable (violent crime rate per 100,000 population)
    data['Crime_Rate'] = data['Violent crime'] / data['Total_Population'] * 100000
    target = 'Crime_Rate'
    
    # Split data into features and target
    X = data[features]
    y = data[target]
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train Random Forest model
    print("Training Random Forest model...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_scaled, y)
    
    # Print feature importance
    feature_importance = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 5 Most Important Features:")
    print(feature_importance.head())
    
    return model, scaler, features

def predict_crime_rate(model, scaler, features, input_data):
    """Make predictions using the trained model"""
    # Scale input data
    input_scaled = scaler.transform(input_data[features])
    
    # Make prediction
    prediction = model.predict(input_scaled)
    
    return prediction

# Train the model when the module is imported
print("Initializing crime prediction model...")
model, scaler, features = train_model()

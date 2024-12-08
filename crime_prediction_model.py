import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score, KFold, GridSearchCV
from sklearn.preprocessing import StandardScaler, RobustScaler, PowerTransformer, QuantileTransformer
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, StackingRegressor
from xgboost import XGBRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.feature_selection import SelectFromModel
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import warnings
warnings.filterwarnings('ignore')

print("Loading and preparing data...")
data = pd.read_csv('US_data/detailed_county_wise_crime_data.csv')

# Rename columns with more descriptive names
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

# Select relevant socioeconomic features
features = [
    'Per_Capita_Income',
    'Poverty_Rate',
    'Less_Than_HS_Education',
    'Some_College_Education',
    'College_Plus_Education',
    'Home_Ownership_Rate',
    'Unemployment_Rate',
    'Diversity_Index',
    'African_American_Pct',
    'Hispanic_Latino_Pct',
    'Median_Age_Total',
    'Total_Population'
]

target = 'Violent_Crime_Rate'

# Prepare the data
X = data[features].copy()
y = data[target].copy()

print("\nPerforming advanced feature engineering...")

# 1. Transform skewed features
for col in ['Total_Population', 'Per_Capita_Income']:
    X[col] = np.log1p(X[col])

# 2. Create interaction terms
X['Poverty_Education'] = X['Poverty_Rate'] * X['Less_Than_HS_Education']
X['Income_Education'] = X['Per_Capita_Income'] * X['College_Plus_Education']
X['Employment_Education'] = X['Unemployment_Rate'] * X['College_Plus_Education']
X['Population_Density'] = X['Total_Population'] / data['Available_Housing_Units']
X['Crime_Risk_Index'] = (X['Poverty_Rate'] * X['Unemployment_Rate'] * (100 - X['College_Plus_Education'])) / 1000

# 3. Create composite features
X['Education_Index'] = (X['College_Plus_Education'] * 3 + X['Some_College_Education'] * 2 - X['Less_Than_HS_Education']) / 6
X['Economic_Stress'] = (X['Poverty_Rate'] + X['Unemployment_Rate']) / 2
X['Social_Stability'] = (X['Home_Ownership_Rate'] - X['Unemployment_Rate'] + X['College_Plus_Education']) / 3

# Handle missing values with sophisticated approach
print("Handling missing values...")
for column in X.columns:
    if X[column].isnull().any():
        if column in ['Per_Capita_Income', 'Total_Population']:
            X[column].fillna(X[column].median(), inplace=True)
        else:
            X[column].fillna(X[column].mean(), inplace=True)

y.fillna(y.median(), inplace=True)

# Remove outliers using IQR method
def remove_outliers(df, columns, n_std=3):
    for column in columns:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        df = df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
    return df

print("Removing outliers...")
combined_data = pd.concat([X, y], axis=1)
combined_data = remove_outliers(combined_data, [target])
X = combined_data[X.columns]
y = combined_data[target]

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('num', RobustScaler(), X.columns)
    ])

# Define base models with optimized hyperparameters
rf_model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42
)

xgb_model = XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)

gb_model = GradientBoostingRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    random_state=42
)

nn_model = MLPRegressor(
    hidden_layer_sizes=(100, 50, 25),
    activation='relu',
    solver='adam',
    alpha=0.0001,
    max_iter=1000,
    random_state=42
)

# Create stacking ensemble
estimators = [
    ('rf', rf_model),
    ('xgb', xgb_model),
    ('gb', gb_model)
]

stack_model = StackingRegressor(
    estimators=estimators,
    final_estimator=xgb_model,
    cv=5
)

# Train and evaluate models with cross-validation
print("\nTraining models with cross-validation...")
kfold = KFold(n_splits=5, shuffle=True, random_state=42)

def evaluate_model(model, name):
    cv_scores = cross_val_score(model, X_train, y_train, cv=kfold, scoring='r2')
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    return {
        'name': name,
        'cv_mean': cv_scores.mean(),
        'cv_std': cv_scores.std(),
        'r2': r2,
        'mse': mse,
        'mae': mae,
        'model': model,
        'predictions': y_pred
    }

models = {
    'Random Forest': rf_model,
    'XGBoost': xgb_model,
    'Gradient Boosting': gb_model,
    'Neural Network': nn_model,
    'Stacking Ensemble': stack_model
}

results = []
for name, model in models.items():
    print(f"Training {name}...")
    result = evaluate_model(model, name)
    results.append(result)
    print(f"{name} - CV R² Score: {result['cv_mean']:.4f} (±{result['cv_std']:.4f})")
    print(f"{name} - Test R² Score: {result['r2']:.4f}, MSE: {result['mse']:.2f}, MAE: {result['mae']:.2f}")

# Select best model
best_result = max(results, key=lambda x: x['r2'])
best_model = best_result['model']
best_model_name = best_result['name']

print(f"\nBest performing model: {best_model_name}")
print(f"Best R² Score: {best_result['r2']:.4f}")

# Feature importance analysis (for tree-based models)
if hasattr(best_model, 'feature_importances_'):
    importance_values = best_model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': importance_values
    })
    feature_importance = feature_importance.sort_values('importance', ascending=False)
    
    print("\nTop 10 Most Important Features:")
    print(feature_importance.head(10).to_string())

# Create visualizations
print("\nGenerating visualizations...")

# 1. Feature Importance Plot (for tree-based models)
if hasattr(best_model, 'feature_importances_'):
    plt.figure(figsize=(12, 8))
    sns.barplot(x='importance', y='feature', data=feature_importance)
    plt.title(f'Feature Importance for Violent Crime Rate Prediction ({best_model_name})')
    plt.tight_layout()
    plt.savefig('feature_importance.png')
    plt.close()

# 2. Correlation Matrix with enhanced visualization
plt.figure(figsize=(15, 12))
correlation_matrix = X.corr()
mask = np.triu(np.ones_like(correlation_matrix), k=1)
sns.heatmap(correlation_matrix, mask=mask, annot=True, cmap='coolwarm', center=0, fmt='.2f')
plt.title('Correlation Matrix of Features')
plt.tight_layout()
plt.savefig('correlation_matrix.png')
plt.close()

# 3. Actual vs Predicted Plot with confidence intervals
plt.figure(figsize=(10, 6))
predictions = best_result['predictions']
plt.scatter(y_test, predictions, alpha=0.5)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
plt.xlabel('Actual Violent Crime Rate')
plt.ylabel('Predicted Violent Crime Rate')
plt.title(f'Actual vs Predicted Violent Crime Rate ({best_model_name})')
plt.tight_layout()
plt.savefig('prediction_accuracy.png')
plt.close()

# 4. Residual Plot with LOWESS smoothing
plt.figure(figsize=(10, 6))
residuals = y_test - predictions
plt.scatter(predictions, residuals, alpha=0.5)
plt.axhline(y=0, color='r', linestyle='--')
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residual Plot')
plt.tight_layout()
plt.savefig('residual_plot.png')
plt.close()

# 5. Cross-validation scores distribution
plt.figure(figsize=(10, 6))
cv_scores = [result['cv_mean'] for result in results]
cv_stds = [result['cv_std'] for result in results]
model_names = [result['name'] for result in results]

plt.errorbar(model_names, cv_scores, yerr=cv_stds, fmt='o')
plt.xticks(rotation=45)
plt.title('Cross-validation Scores by Model')
plt.ylabel('R² Score')
plt.tight_layout()
plt.savefig('cv_scores.png')
plt.close()

def predict_violent_crime(new_data):
    """
    Make predictions for new data.
    new_data should be a dictionary with the same features used in training.
    """
    # Create DataFrame from input
    new_df = pd.DataFrame([new_data])
    
    # Apply the same transformations as training data
    if 'Total_Population' in new_df.columns:
        new_df['Total_Population'] = np.log1p(new_df['Total_Population'])
    if 'Per_Capita_Income' in new_df.columns:
        new_df['Per_Capita_Income'] = np.log1p(new_df['Per_Capita_Income'])
    
    # Create interaction terms
    new_df['Poverty_Education'] = new_df['Poverty_Rate'] * new_df['Less_Than_HS_Education']
    new_df['Income_Education'] = new_df['Per_Capita_Income'] * new_df['College_Plus_Education']
    new_df['Employment_Education'] = new_df['Unemployment_Rate'] * new_df['College_Plus_Education']
    new_df['Population_Density'] = new_df['Total_Population'] / new_df['Available_Housing_Units']
    new_df['Crime_Risk_Index'] = (new_df['Poverty_Rate'] * new_df['Unemployment_Rate'] * (100 - new_df['College_Plus_Education'])) / 1000
    
    # Create composite features
    new_df['Education_Index'] = (new_df['College_Plus_Education'] * 3 + new_df['Some_College_Education'] * 2 - new_df['Less_Than_HS_Education']) / 6
    new_df['Economic_Stress'] = (new_df['Poverty_Rate'] + new_df['Unemployment_Rate']) / 2
    new_df['Social_Stability'] = (new_df['Home_Ownership_Rate'] - new_df['Unemployment_Rate'] + new_df['College_Plus_Education']) / 3
    
    # Drop columns not used in training
    new_df = new_df.drop(['Available_Housing_Units'], axis=1)
    
    # Make prediction
    prediction = best_model.predict(new_df)
    return prediction[0]

# Example usage of the prediction function
example_county = {
    'Per_Capita_Income': 25000,
    'Poverty_Rate': 15,
    'Less_Than_HS_Education': 10,
    'Some_College_Education': 30,
    'College_Plus_Education': 25,
    'Home_Ownership_Rate': 70,
    'Unemployment_Rate': 5,
    'Diversity_Index': 0.5,
    'African_American_Pct': 12,
    'Hispanic_Latino_Pct': 15,
    'Median_Age_Total': 38,
    'Total_Population': 50000,
    'Available_Housing_Units': 20000
}

print("\nExample Prediction:")
print(f"Predicted violent crime rate for example county: {predict_violent_crime(example_county):.2f} per 100,000 population")

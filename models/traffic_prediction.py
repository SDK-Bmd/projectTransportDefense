"""
Traffic prediction models for La Défense mobility
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import joblib
from datetime import datetime, timedelta


class TrafficPredictor:
    """Class for predicting traffic conditions in La Défense"""

    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()

    def prepare_features(self, traffic_data, weather_data):
        """Prepare features for the model"""
        # Merge traffic and weather data
        combined_data = pd.merge(
            traffic_data,
            weather_data,
            on='timestamp',
            how='inner'
        )

        # Extract time features
        combined_data['hour'] = pd.to_datetime(combined_data['timestamp']).dt.hour
        combined_data['day_of_week'] = pd.to_datetime(combined_data['timestamp']).dt.dayofweek
        combined_data['is_weekend'] = (combined_data['day_of_week'] >= 5).astype(int)

        # Select features
        features = combined_data[[
            'hour', 'day_of_week', 'is_weekend',
            'temperature', 'humidity', 'precipitation',
            'wind_speed'
        ]]

        # Target variable: congestion level
        target = combined_data['congestion_level']

        return features, target

    def train(self, traffic_data, weather_data):
        """Train the traffic prediction model"""
        features, target = self.prepare_features(traffic_data, weather_data)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            features, target, test_size=0.2, random_state=42
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Model MAE: {mae:.4f}")

        return mae

    def predict(self, weather_forecast, hour, day_of_week, is_weekend=None):
        """Predict traffic conditions based on weather and time"""
        if is_weekend is None:
            is_weekend = 1 if day_of_week >= 5 else 0

        # Prepare input features
        features = pd.DataFrame({
            'hour': [hour],
            'day_of_week': [day_of_week],
            'is_weekend': [is_weekend],
            'temperature': [weather_forecast['temperature']],
            'humidity': [weather_forecast['humidity']],
            'precipitation': [weather_forecast['precipitation']],
            'wind_speed': [weather_forecast['wind_speed']]
        })

        # Scale and predict
        features_scaled = self.scaler.transform(features)
        prediction = self.model.predict(features_scaled)[0]

        return prediction

    def save_model(self, path):
        """Save model to disk"""
        joblib.dump({
            'model': self.model,
            'scaler': self.scaler
        }, path)

    def load_model(self, path):
        """Load model from disk"""
        saved_model = joblib.load(path)
        self.model = saved_model['model']
        self.scaler = saved_model['scaler']
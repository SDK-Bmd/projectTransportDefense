"""
Enhanced prediction model for La Défense mobility using historical data
Trains on 1 month of historical data to predict traffic and transport conditions
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, accuracy_score, classification_report
import joblib
from datetime import datetime, timedelta
import sys
import os

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_lake_utils import read_parquet_from_data_lake, save_parquet_to_data_lake
from configuration.config import DATA_LAKE


class MobilityPredictor:
    """Enhanced mobility predictor using historical data"""

    def __init__(self):
        self.bucket_name = DATA_LAKE["bucket_name"]
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self.feature_columns = []

    def load_training_data(self):
        """Load the historical training dataset"""
        try:
            df = read_parquet_from_data_lake(
                self.bucket_name,
                "analytics/training/mobility_training_dataset.parquet"
            )

            if df.empty:
                print("No training data found. Please run historical data extraction first.")
                return None

            print(f"Loaded training dataset with {len(df)} records")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Transport types: {df['transport_type'].unique()}")
            print(f"Lines: {df['line'].unique()}")

            return df

        except Exception as e:
            print(f"Error loading training data: {str(e)}")
            return None

    def prepare_features(self, df):
        """Prepare features for ML models"""
        print("Preparing features for training...")

        # Create feature dataframe
        features_df = df.copy()

        # Encode categorical variables
        if 'transport_type' not in self.encoders:
            self.encoders['transport_type'] = LabelEncoder()
            features_df['transport_type_encoded'] = self.encoders['transport_type'].fit_transform(
                features_df['transport_type'])
        else:
            features_df['transport_type_encoded'] = self.encoders['transport_type'].transform(
                features_df['transport_type'])

        if 'line' not in self.encoders:
            self.encoders['line'] = LabelEncoder()
            features_df['line_encoded'] = self.encoders['line'].fit_transform(features_df['line'])
        else:
            features_df['line_encoded'] = self.encoders['line'].transform(features_df['line'])

        # Create time-based features
        features_df['date'] = pd.to_datetime(features_df['date'])
        features_df['month'] = features_df['date'].dt.month
        features_df['day'] = features_df['date'].dt.day
        features_df['is_rush_hour'] = ((features_df['hour'] >= 7) & (features_df['hour'] <= 9)) | \
                                      ((features_df['hour'] >= 17) & (features_df['hour'] <= 19))
        features_df['is_business_hours'] = (features_df['hour'] >= 9) & (features_df['hour'] <= 17)

        # Weather impact features
        features_df['high_precipitation'] = features_df['precipitation'] > 5
        features_df['low_visibility'] = features_df['visibility'] < 5
        features_df['high_wind'] = features_df['wind_speed'] > 30
        features_df['extreme_temp'] = (features_df['temperature'] < 5) | (features_df['temperature'] > 30)

        # Interaction features
        features_df['weather_impact_score'] = (
                features_df['high_precipitation'].astype(int) * 2 +
                features_df['low_visibility'].astype(int) * 1.5 +
                features_df['high_wind'].astype(int) * 1 +
                features_df['extreme_temp'].astype(int) * 0.5
        )

        # Feature columns for ML
        self.feature_columns = [
            'hour', 'day_of_week', 'is_weekend', 'month', 'day',
            'transport_type_encoded', 'line_encoded',
            'is_rush_hour', 'is_business_hours',
            'temperature', 'humidity', 'precipitation', 'wind_speed', 'pressure', 'visibility',
            'high_precipitation', 'low_visibility', 'high_wind', 'extreme_temp', 'weather_impact_score',
            'avg_road_congestion'
        ]

        return features_df

    def train_transport_reliability_model(self, df):
        """Train model to predict transport reliability"""
        print("Training transport reliability prediction model...")

        # Prepare features and target
        X = df[self.feature_columns]
        y = df['transport_reliability']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=df['transport_type']
        )

        # Scale features
        self.scalers['reliability'] = StandardScaler()
        X_train_scaled = self.scalers['reliability'].fit_transform(X_train)
        X_test_scaled = self.scalers['reliability'].transform(X_test)

        # Train model
        self.models['reliability'] = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            random_state=42
        )

        self.models['reliability'].fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.models['reliability'].predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)

        print(f"Transport Reliability Model - MAE: {mae:.4f}")

        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': self.models['reliability'].feature_importances_
        }).sort_values('importance', ascending=False)

        print("Top 5 most important features for reliability:")
        print(feature_importance.head())

        return mae

    def train_delay_prediction_model(self, df):
        """Train model to predict transport delays"""
        print("Training delay prediction model...")

        # Prepare features and target
        X = df[self.feature_columns]
        y = df['transport_delays']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=df['transport_type']
        )

        # Scale features
        self.scalers['delays'] = StandardScaler()
        X_train_scaled = self.scalers['delays'].fit_transform(X_train)
        X_test_scaled = self.scalers['delays'].transform(X_test)

        # Train model
        self.models['delays'] = RandomForestRegressor(
            n_estimators=100,
            max_depth=12,
            min_samples_split=5,
            random_state=42
        )

        self.models['delays'].fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.models['delays'].predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)

        print(f"Delay Prediction Model - MAE: {mae:.4f} minutes")

        return mae

    def train_congestion_classification_model(self, df):
        """Train model to classify road congestion levels"""
        print("Training congestion classification model...")

        # Create congestion level categories
        df['congestion_category'] = pd.cut(
            df['avg_road_congestion'],
            bins=[-0.1, 1, 2, 3, 5],
            labels=['Low', 'Moderate', 'High', 'Very High']
        )

        # Prepare features and target
        X = df[self.feature_columns]
        y = df['congestion_category']

        # Remove samples with NaN target
        mask = ~y.isna()
        X = X[mask]
        y = y[mask]

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Scale features
        self.scalers['congestion'] = StandardScaler()
        X_train_scaled = self.scalers['congestion'].fit_transform(X_train)
        X_test_scaled = self.scalers['congestion'].transform(X_test)

        # Train model
        self.models['congestion'] = RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            random_state=42
        )

        self.models['congestion'].fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.models['congestion'].predict(X_test_scaled)
        accuracy = accuracy_score(y_test, y_pred)

        print(f"Congestion Classification Model - Accuracy: {accuracy:.4f}")
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))

        return accuracy

    def train_passenger_load_model(self, df):
        """Train model to predict passenger load"""
        print("Training passenger load prediction model...")

        # Prepare features and target
        X = df[self.feature_columns]
        y = df['passenger_load']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=df['transport_type']
        )

        # Scale features
        self.scalers['passenger_load'] = StandardScaler()
        X_train_scaled = self.scalers['passenger_load'].fit_transform(X_train)
        X_test_scaled = self.scalers['passenger_load'].transform(X_test)

        # Train model
        self.models['passenger_load'] = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            random_state=42
        )

        self.models['passenger_load'].fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.models['passenger_load'].predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)

        print(f"Passenger Load Model - MAE: {mae:.4f}")

        return mae

    def save_models(self):
        """Save trained models and preprocessors"""
        try:
            model_data = {
                'models': self.models,
                'scalers': self.scalers,
                'encoders': self.encoders,
                'feature_columns': self.feature_columns,
                'training_date': datetime.now().isoformat()
            }

            # Save using joblib
            joblib.dump(model_data, 'mobility_prediction_models.pkl')
            print("✅ Models saved successfully to mobility_prediction_models.pkl")

            # Also save model metadata
            metadata = {
                'model_types': list(self.models.keys()),
                'feature_count': len(self.feature_columns),
                'training_date': datetime.now().isoformat(),
                'feature_columns': self.feature_columns
            }

            import json
            with open('model_metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)

            return True

        except Exception as e:
            print(f"Error saving models: {str(e)}")
            return False

    def load_models(self):
        """Load previously trained models"""
        try:
            model_data = joblib.load('mobility_prediction_models.pkl')

            self.models = model_data['models']
            self.scalers = model_data['scalers']
            self.encoders = model_data['encoders']
            self.feature_columns = model_data['feature_columns']

            print("✅ Models loaded successfully")
            print(f"Available models: {list(self.models.keys())}")

            return True

        except Exception as e:
            print(f"Error loading models: {str(e)}")
            return False

    def predict_conditions(self, datetime_input, transport_type, line, weather_conditions):
        """
        Predict mobility conditions for given inputs

        Args:
            datetime_input: datetime object for prediction
            transport_type: 'metro', 'rers', 'transilien', 'buses'
            line: line identifier (e.g., '1', 'A', 'E', 'L', '144')
            weather_conditions: dict with weather data

        Returns:
            dict with predictions
        """
        try:
            # Prepare input features
            input_data = {
                'hour': datetime_input.hour,
                'day_of_week': datetime_input.weekday(),
                'is_weekend': datetime_input.weekday() >= 5,
                'month': datetime_input.month,
                'day': datetime_input.day,
                'is_rush_hour': (7 <= datetime_input.hour <= 9) or (17 <= datetime_input.hour <= 19),
                'is_business_hours': 9 <= datetime_input.hour <= 17,
                'temperature': weather_conditions.get('temperature', 15),
                'humidity': weather_conditions.get('humidity', 70),
                'precipitation': weather_conditions.get('precipitation', 0),
                'wind_speed': weather_conditions.get('wind_speed', 10),
                'pressure': weather_conditions.get('pressure', 1013),
                'visibility': weather_conditions.get('visibility', 10),
                'avg_road_congestion': 2  # Default moderate congestion
            }

            # Encode categorical variables
            if transport_type in self.encoders['transport_type'].classes_:
                input_data['transport_type_encoded'] = self.encoders['transport_type'].transform([transport_type])[0]
            else:
                input_data['transport_type_encoded'] = 0

            if line in self.encoders['line'].classes_:
                input_data['line_encoded'] = self.encoders['line'].transform([line])[0]
            else:
                input_data['line_encoded'] = 0

            # Weather impact features
            input_data['high_precipitation'] = input_data['precipitation'] > 5
            input_data['low_visibility'] = input_data['visibility'] < 5
            input_data['high_wind'] = input_data['wind_speed'] > 30
            input_data['extreme_temp'] = (input_data['temperature'] < 5) or (input_data['temperature'] > 30)

            input_data['weather_impact_score'] = (
                    int(input_data['high_precipitation']) * 2 +
                    int(input_data['low_visibility']) * 1.5 +
                    int(input_data['high_wind']) * 1 +
                    int(input_data['extreme_temp']) * 0.5
            )

            # Create feature vector
            feature_vector = np.array([input_data[col] for col in self.feature_columns]).reshape(1, -1)

            predictions = {}

            # Make predictions with each model
            if 'reliability' in self.models:
                X_scaled = self.scalers['reliability'].transform(feature_vector)
                reliability = self.models['reliability'].predict(X_scaled)[0]
                predictions['reliability'] = max(0, min(1, reliability))

            if 'delays' in self.models:
                X_scaled = self.scalers['delays'].transform(feature_vector)
                delays = self.models['delays'].predict(X_scaled)[0]
                predictions['expected_delay_minutes'] = max(0, delays)

            if 'passenger_load' in self.models:
                X_scaled = self.scalers['passenger_load'].transform(feature_vector)
                load = self.models['passenger_load'].predict(X_scaled)[0]
                predictions['passenger_load'] = max(0, min(1, load))

            if 'congestion' in self.models:
                X_scaled = self.scalers['congestion'].transform(feature_vector)
                congestion_proba = self.models['congestion'].predict_proba(X_scaled)[0]
                congestion_class = self.models['congestion'].predict(X_scaled)[0]
                predictions['congestion_level'] = congestion_class
                predictions['congestion_probability'] = dict(zip(
                    self.models['congestion'].classes_,
                    congestion_proba
                ))

            return predictions

        except Exception as e:
            print(f"Error making predictions: {str(e)}")
            return {}

    def predict_next_24_hours(self, transport_type, line, weather_forecast):
        """
        Predict conditions for the next 24 hours

        Args:
            transport_type: Transport type to predict
            line: Line to predict
            weather_forecast: List of hourly weather conditions

        Returns:
            DataFrame with hourly predictions
        """
        predictions = []
        current_time = datetime.now()

        for hour_offset in range(24):
            future_time = current_time + timedelta(hours=hour_offset)

            # Get weather for this hour (or use default)
            if hour_offset < len(weather_forecast):
                weather = weather_forecast[hour_offset]
            else:
                weather = {
                    'temperature': 15,
                    'humidity': 70,
                    'precipitation': 0,
                    'wind_speed': 10,
                    'pressure': 1013,
                    'visibility': 10
                }

            hourly_prediction = self.predict_conditions(
                future_time, transport_type, line, weather
            )

            hourly_prediction['datetime'] = future_time
            hourly_prediction['hour'] = future_time.hour
            hourly_prediction['transport_type'] = transport_type
            hourly_prediction['line'] = line

            predictions.append(hourly_prediction)

        return pd.DataFrame(predictions)

    def train_all_models(self):
        """Train all prediction models"""
        print("Starting comprehensive model training...")

        # Load training data
        df = self.load_training_data()
        if df is None:
            return False

        # Prepare features
        df_processed = self.prepare_features(df)

        # Train all models
        results = {}

        try:
            results['reliability'] = self.train_transport_reliability_model(df_processed)
            results['delays'] = self.train_delay_prediction_model(df_processed)
            results['congestion'] = self.train_congestion_classification_model(df_processed)
            results['passenger_load'] = self.train_passenger_load_model(df_processed)

            # Save models
            if self.save_models():
                print("\n✅ All models trained and saved successfully!")
                print("📊 Model Performance Summary:")
                for model_name, score in results.items():
                    print(f"  - {model_name}: {score:.4f}")
                return True
            else:
                print("❌ Error saving models")
                return False

        except Exception as e:
            print(f"❌ Error during model training: {str(e)}")
            return False


class PredictionService:
    """Service class for making real-time predictions"""

    def __init__(self):
        self.predictor = MobilityPredictor()
        self.models_loaded = False

    def initialize(self):
        """Initialize the prediction service"""
        if os.path.exists('mobility_prediction_models.pkl'):
            self.models_loaded = self.predictor.load_models()
            if self.models_loaded:
                print("✅ Prediction service initialized successfully")
            else:
                print("❌ Failed to load models")
        else:
            print("⚠️ No trained models found. Please train models first.")

        return self.models_loaded

    def get_transport_prediction(self, transport_type, line, hours_ahead=1):
        """Get prediction for specific transport line"""
        if not self.models_loaded:
            return None

        future_time = datetime.now() + timedelta(hours=hours_ahead)

        # Get current weather (simplified - in production, use real weather API)
        weather = {
            'temperature': 15,
            'humidity': 70,
            'precipitation': 0,
            'wind_speed': 10,
            'pressure': 1013,
            'visibility': 10
        }

        return self.predictor.predict_conditions(future_time, transport_type, line, weather)

    def get_24h_forecast(self, transport_type, line):
        """Get 24-hour forecast for transport line"""
        if not self.models_loaded:
            return pd.DataFrame()

        # Default weather forecast (in production, get from weather API)
        weather_forecast = [
            {
                'temperature': 15 + (i % 6 - 3) * 2,  # Some variation
                'humidity': 70,
                'precipitation': 0 if i % 8 != 0 else 2,  # Occasional rain
                'wind_speed': 10,
                'pressure': 1013,
                'visibility': 10
            }
            for i in range(24)
        ]

        return self.predictor.predict_next_24_hours(transport_type, line, weather_forecast)


def main():
    """Main function to train models or run predictions"""
    import argparse

    parser = argparse.ArgumentParser(description='La Défense Mobility Prediction System')
    parser.add_argument('--train', action='store_true', help='Train new models')
    parser.add_argument('--predict', action='store_true', help='Run prediction service')
    parser.add_argument('--transport-type', default='metro', help='Transport type for prediction')
    parser.add_argument('--line', default='1', help='Line for prediction')

    args = parser.parse_args()

    if args.train:
        print("🤖 Training mobility prediction models...")
        predictor = MobilityPredictor()
        success = predictor.train_all_models()

        if success:
            print("✅ Model training completed successfully!")
        else:
            print("❌ Model training failed")

    elif args.predict:
        print("🔮 Running prediction service...")
        service = PredictionService()

        if service.initialize():
            # Example prediction
            prediction = service.get_transport_prediction(args.transport_type, args.line)
            print(f"\n📊 Prediction for {args.transport_type} {args.line}:")
            for key, value in prediction.items():
                print(f"  {key}: {value}")

            # 24h forecast
            forecast = service.get_24h_forecast(args.transport_type, args.line)
            if not forecast.empty:
                print(f"\n📈 24-hour reliability forecast:")
                for _, row in forecast.head(6).iterrows():  # Show first 6 hours
                    print(f"  {row['datetime'].strftime('%H:%M')}: {row.get('reliability', 'N/A'):.2f}")
        else:
            print("❌ Failed to initialize prediction service")

    else:
        print("Please specify --train or --predict")


if __name__ == "__main__":
    main()
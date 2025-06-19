import numpy as np
import pandas as pd
from datetime import datetime
import logging

class WeatherImpactCalculator:
    """
    Advanced weather impact calculator using multiple meteorological factors
    and transport mode-specific sensitivities
    """

    def __init__(self):
        # Base impact factors per transport mode (research-based)
        self.base_sensitivity = {
            'metro': 0.05,    # Very low impact (underground)
            'rer': 0.10,      # Low impact (mostly underground/covered)
            'tram': 0.25,     # Medium impact (surface level)
            'bus': 0.35,      # High impact (surface, traffic dependent)
            'car': 0.40,      # Very high impact (traffic congestion)
            'walking': 0.60,  # Highest impact (direct exposure)
            'cycling': 0.55   # High impact (direct exposure)
        }

        # Weather condition thresholds and impact coefficients
        self.weather_thresholds = {
            'precipitation': {
                'light': (0.1, 2.5),     # 0.1-2.5mm/h
                'moderate': (2.5, 10),    # 2.5-10mm/h
                'heavy': (10, 50),        # 10-50mm/h
                'extreme': (50, 200)      # >50mm/h
            },
            'wind_speed': {
                'light': (10, 25),       # 10-25 km/h
                'moderate': (25, 50),     # 25-50 km/h
                'strong': (50, 75),       # 50-75 km/h
                'extreme': (75, 150)      # >75 km/h
            },
            'temperature': {
                'extreme_cold': (-20, -10), # < -10°C
                'cold': (-10, 0),           # -10 to 0°C
                'normal': (0, 30),          # 0 to 30°C
                'hot': (30, 35),            # 30-35°C
                'extreme_hot': (35, 50)     # > 35°C
            },
            'visibility': {
                'poor': (0, 1),           # < 1km (heavy fog)
                'reduced': (1, 5),        # 1-5km (fog/mist)
                'moderate': (5, 10),      # 5-10km (light fog)
                'good': (10, 50)          # > 10km (clear)
            }
        }

        # Impact multipliers for each condition level
        self.impact_multipliers = {
            'precipitation': {
                'none': 1.0,
                'light': 1.15,
                'moderate': 1.35,
                'heavy': 1.65,
                'extreme': 2.2
            },
            'wind_speed': {
                'calm': 1.0,
                'light': 1.05,
                'moderate': 1.25,
                'strong': 1.55,
                'extreme': 2.0
            },
            'temperature': {
                'normal': 1.0,
                'cold': 1.10,
                'extreme_cold': 1.30,
                'hot': 1.15,
                'extreme_hot': 1.40
            },
            'visibility': {
                'good': 1.0,
                'moderate': 1.20,
                'reduced': 1.50,
                'poor': 2.10
            }
        }

        # Time-based modifiers (rush hour vs off-peak)
        self.time_modifiers = {
            'rush_hour': 1.3,      # 7-9h, 17-19h
            'peak': 1.15,          # 6-7h, 9-11h, 16-17h, 19-21h
            'off_peak': 1.0        # Other times
        }

    def categorize_weather_condition(self, value, condition_type):
        """Categorize weather values into impact levels"""
        thresholds = self.weather_thresholds[condition_type]

        if condition_type == 'precipitation':
            if value <= 0.1:
                return 'none'
            elif value <= thresholds['light'][1]:
                return 'light'
            elif value <= thresholds['moderate'][1]:
                return 'moderate'
            elif value <= thresholds['heavy'][1]:
                return 'heavy'
            else:
                return 'extreme'

        elif condition_type == 'wind_speed':
            if value < thresholds['light'][0]:
                return 'calm'
            elif value <= thresholds['light'][1]:
                return 'light'
            elif value <= thresholds['moderate'][1]:
                return 'moderate'
            elif value <= thresholds['strong'][1]:
                return 'strong'
            else:
                return 'extreme'

        elif condition_type == 'temperature':
            if thresholds['normal'][0] <= value <= thresholds['normal'][1]:
                return 'normal'
            elif value < thresholds['extreme_cold'][1]:
                return 'extreme_cold'
            elif value < thresholds['cold'][1]:
                return 'cold'
            elif value <= thresholds['hot'][1]:
                return 'hot'
            else:
                return 'extreme_hot'

        elif condition_type == 'visibility':
            if value >= thresholds['good'][0]:
                return 'good'
            elif value >= thresholds['moderate'][0]:
                return 'moderate'
            elif value >= thresholds['reduced'][0]:
                return 'reduced'
            else:
                return 'poor'

    def get_time_modifier(self, hour):
        """Get time-based impact modifier"""
        if hour in [7, 8, 17, 18]:  # Rush hours
            return self.time_modifiers['rush_hour']
        elif hour in [6, 9, 10, 16, 19, 20]:  # Peak hours
            return self.time_modifiers['peak']
        else:
            return self.time_modifiers['off_peak']

    def calculate_compound_weather_impact(self, weather_data):
        """
        Calculate compound weather impact using multiple factors

        Formula:
        Impact = Base_Sensitivity × (P_mult × W_mult × T_mult × V_mult)^0.7 × Time_modifier

        Where:
        - P_mult = Precipitation multiplier
        - W_mult = Wind speed multiplier
        - T_mult = Temperature multiplier
        - V_mult = Visibility multiplier
        - ^0.7 = Damping factor to prevent extreme values
        """

        # Extract weather values with defaults
        precipitation = weather_data.get('precipitation', 0)
        wind_speed = weather_data.get('wind_speed', 0)
        temperature = weather_data.get('temperature', 15)
        visibility = weather_data.get('visibility', 10)
        hour = weather_data.get('hour', 12)

        # Categorize conditions
        precip_category = self.categorize_weather_condition(precipitation, 'precipitation')
        wind_category = self.categorize_weather_condition(wind_speed, 'wind_speed')
        temp_category = self.categorize_weather_condition(temperature, 'temperature')
        vis_category = self.categorize_weather_condition(visibility, 'visibility')

        # Get multipliers
        precip_mult = self.impact_multipliers['precipitation'][precip_category]
        wind_mult = self.impact_multipliers['wind_speed'][wind_category]
        temp_mult = self.impact_multipliers['temperature'][temp_category]
        vis_mult = self.impact_multipliers['visibility'][vis_category]
        time_mult = self.get_time_modifier(hour)

        # Calculate compound impact for each transport mode
        impacts = {}

        for transport_mode, base_sens in self.base_sensitivity.items():
            # Compound weather factor with damping
            weather_factor = (precip_mult * wind_mult * temp_mult * vis_mult) ** 0.7

            # Final impact calculation
            impact_multiplier = 1.0 + (base_sens * (weather_factor - 1.0) * time_mult)

            # Cap the maximum impact to prevent unrealistic values
            impact_multiplier = min(impact_multiplier, 3.0)

            impacts[transport_mode] = {
                'impact_multiplier': round(impact_multiplier, 3),
                'travel_time_increase_pct': round((impact_multiplier - 1.0) * 100, 1),
                'weather_breakdown': {
                    'precipitation': f"{precip_category} ({precipitation}mm/h)",
                    'wind': f"{wind_category} ({wind_speed}km/h)",
                    'temperature': f"{temp_category} ({temperature}°C)",
                    'visibility': f"{vis_category} ({visibility}km)"
                },
                'time_factor': f"{'rush_hour' if time_mult == 1.3 else 'peak' if time_mult == 1.15 else 'off_peak'}"
            }

        return impacts

    def calculate_economic_impact(self, weather_impacts, traffic_volume, avg_trip_duration_min=25):
        """
        Calculate economic impact of weather on transportation

        Based on:
        - Increased travel times
        - Fuel consumption
        - Productivity losses
        """

        # Economic constants (EUR)
        time_value_per_hour = 25.0  # Average time value
        fuel_cost_per_liter = 1.75
        fuel_consumption_increase_per_pct = 0.003  # L/km per % delay
        avg_trip_distance_km = 12  # Average trip distance in La Défense

        economic_impacts = {}

        for mode, impact_data in weather_impacts.items():
            if mode in ['walking', 'cycling']:  # Skip non-motorized modes
                continue

            multiplier = impact_data['impact_multiplier']
            delay_minutes = avg_trip_duration_min * (multiplier - 1.0)

            # Time cost
            time_cost_per_trip = (delay_minutes / 60) * time_value_per_hour

            # Fuel cost (for motorized transport)
            if mode in ['car', 'bus']:
                fuel_increase_pct = (multiplier - 1.0) * 100
                additional_fuel_cost = (fuel_increase_pct * fuel_consumption_increase_per_pct *
                                       avg_trip_distance_km * fuel_cost_per_liter)
            else:
                additional_fuel_cost = 0

            total_cost_per_trip = time_cost_per_trip + additional_fuel_cost
            daily_impact = total_cost_per_trip * traffic_volume

            economic_impacts[mode] = {
                'delay_minutes': round(delay_minutes, 1),
                'time_cost_per_trip_eur': round(time_cost_per_trip, 2),
                'fuel_cost_per_trip_eur': round(additional_fuel_cost, 2),
                'total_cost_per_trip_eur': round(total_cost_per_trip, 2),
                'daily_economic_impact_eur': round(daily_impact, 0)
            }

        return economic_impacts

    def generate_recommendations(self, weather_impacts):
        """Generate transport recommendations based on weather impact"""

        # Sort transport modes by impact (lower is better)
        sorted_modes = sorted(weather_impacts.items(),
                            key=lambda x: x[1]['impact_multiplier'])

        recommendations = []

        for mode, impact_data in sorted_modes[:3]:  # Top 3 recommendations
            impact_pct = impact_data['travel_time_increase_pct']

            if impact_pct < 5:
                priority = "Recommended"
                color = "green"
            elif impact_pct < 15:
                priority = "Good option"
                color = "orange"
            else:
                priority = "Consider alternatives"
                color = "red"

            recommendations.append({
                'transport_mode': mode.title(),
                'priority': priority,
                'impact_increase_pct': impact_pct,
                'multiplier': impact_data['impact_multiplier'],
                'color': color,
                'weather_factors': impact_data['weather_breakdown']
            })

        return recommendations

# Example usage function
def analyze_current_weather_impact(current_weather_data, traffic_volume=1000):
    """
    Main function to analyze weather impact on La Défense transportation

    Args:
        current_weather_data: Dict containing weather information
        traffic_volume: Number of trips to consider for economic impact

    Returns:
        Complete analysis including impacts, economics, and recommendations
    """

    calculator = WeatherImpactCalculator()

    # Calculate weather impacts
    weather_impacts = calculator.calculate_compound_weather_impact(current_weather_data)

    # Calculate economic impact
    economic_impacts = calculator.calculate_economic_impact(weather_impacts, traffic_volume)

    # Generate recommendations
    recommendations = calculator.generate_recommendations(weather_impacts)

    return {
        'weather_impacts': weather_impacts,
        'economic_impacts': economic_impacts,
        'recommendations': recommendations,
        'analysis_timestamp': datetime.now().isoformat()
    }


# Test with sample data
if __name__ == "__main__":
    # Sample weather data for testing
    test_weather = {
        'precipitation': 8.5,      # Heavy rain
        'wind_speed': 35,          # Moderate wind
        'temperature': 5,          # Cold
        'visibility': 3,           # Reduced visibility
        'hour': 8                  # Rush hour
    }

    results = analyze_current_weather_impact(test_weather, traffic_volume=1500)

    print("=== Weather Impact Analysis ===")
    print(f"Weather conditions: {test_weather}")
    print("\n=== Transport Mode Impacts ===")
    for mode, data in results['weather_impacts'].items():
        print(f"{mode.title()}: {data['impact_multiplier']}x "
              f"({data['travel_time_increase_pct']}% increase)")

    print("\n=== Recommendations ===")
    for rec in results['recommendations']:
        print(f"{rec['priority']}: {rec['transport_mode']} "
              f"({rec['impact_increase_pct']}% impact)")
"""
Environmental impact analysis for La Défense mobility
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class EnvironmentalImpactAnalyzer:
    """Class for analyzing environmental impacts of transportation in La Défense"""

    def __init__(self):
        self.emission_factors = {
            'car': 120,  # g CO2 per km per person
            'bus': 70,  # g CO2 per km per person
            'metro': 4,  # g CO2 per km per person
            'rer': 6,  # g CO2 per km per person
            'tram': 3,  # g CO2 per km per person
            'walking': 0,
            'cycling': 0
        }

    def calculate_route_emissions(self, route_details, num_passengers=1):
        """Calculate CO2 emissions for a given route"""
        total_emissions = 0

        for step in route_details:
            transport_type = step['transport_type'].lower()
            travel_time = step['travel_time']  # in minutes

            # Estimate distance based on travel time and average speed
            if transport_type in ['metro', 'rer', 'tram']:
                avg_speed = 30  # km/h for rail
            elif transport_type == 'bus':
                avg_speed = 15  # km/h for bus
            elif transport_type == 'car':
                avg_speed = 25  # km/h for car in urban area
            else:
                avg_speed = 5  # km/h for walking/cycling

            distance = (avg_speed * travel_time) / 60  # Convert to km

            # Get emission factor
            factor = self.emission_factors.get(transport_type, 0)

            # Calculate emissions
            step_emissions = factor * distance * num_passengers
            total_emissions += step_emissions

            # Add emissions to step details
            step['distance_km'] = distance
            step['emissions_g'] = step_emissions

        return total_emissions

    def compare_route_emissions(self, routes):
        """Compare emissions between different route options"""
        results = []

        for route_name, route in routes.items():
            emissions = self.calculate_route_emissions(route['route_details'])
            results.append({
                'route_name': route_name,
                'total_time_min': route['total_time'],
                'emissions_g': emissions,
                'emissions_kg': emissions / 1000
            })

        return pd.DataFrame(results)

    def calculate_congestion_impact(self, congestion_level, traffic_volume, road_length_km):
        """Calculate additional emissions due to congestion"""
        # Baseline emissions at free flow
        baseline_emissions = self.emission_factors['car'] * traffic_volume * road_length_km

        # Additional emissions factor based on congestion level (0-5)
        # Higher congestion = more fuel consumption due to stop-and-go traffic
        congestion_multiplier = 1.0 + (congestion_level * 0.2)  # Up to 2x emissions at highest congestion

        # Calculate total emissions with congestion
        congestion_emissions = baseline_emissions * congestion_multiplier

        # Additional emissions due to congestion
        additional_emissions = congestion_emissions - baseline_emissions

        return {
            'baseline_emissions_kg': baseline_emissions / 1000,
            'congestion_emissions_kg': congestion_emissions / 1000,
            'additional_emissions_kg': additional_emissions / 1000
        }
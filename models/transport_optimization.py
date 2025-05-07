"""
Transport optimization models for recommending optimal routes
"""
import pandas as pd
import numpy as np
import networkx as nx
from datetime import datetime, timedelta


class RouteOptimizer:
    """Class for optimizing routes in La Défense"""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.stations = {}
        self.traffic_predictions = {}

    def build_transport_graph(self, stations_data, routes_data):
        """Build a graph representation of the transport network"""
        # Add stations as nodes
        for _, station in stations_data.iterrows():
            self.graph.add_node(
                station['id'],
                name=station['name'],
                type=station['type'],
                lat=station['lat'],
                lon=station['lon'],
                wheelchair_accessible=station['wheelchair_accessible']
            )
            self.stations[station['id']] = station.to_dict()

        # Add routes as edges
        for _, route in routes_data.iterrows():
            self.graph.add_edge(
                route['from_station_id'],
                route['to_station_id'],
                route_id=route['route_id'],
                transport_type=route['transport_type'],
                line=route['line'],
                travel_time=route['avg_travel_time'],
                congestion_factor=1.0  # Default value, will be updated with predictions
            )

    def update_congestion_factors(self, traffic_predictions):
        """Update congestion factors in the graph based on traffic predictions"""
        self.traffic_predictions = traffic_predictions

        for edge in self.graph.edges:
            from_id, to_id = edge
            edge_data = self.graph.get_edge_data(from_id, to_id)

            # Find matching prediction if available
            route_id = edge_data['route_id']
            if route_id in traffic_predictions:
                congestion_level = traffic_predictions[route_id]
                # Convert congestion level to a multiplier (higher congestion = longer travel time)
                congestion_factor = 1.0 + (congestion_level / 5.0)  # Scale between 1.0 and 3.0
                self.graph[from_id][to_id]['congestion_factor'] = congestion_factor
                self.graph[from_id][to_id]['current_travel_time'] = edge_data['travel_time'] * congestion_factor
            else:
                # No prediction available, use default values
                self.graph[from_id][to_id]['congestion_factor'] = 1.0
                self.graph[from_id][to_id]['current_travel_time'] = edge_data['travel_time']

    def find_optimal_route(self, start_station_id, end_station_id, preferences=None):
        """Find the optimal route between two stations based on preferences"""
        if preferences is None:
            preferences = {'time': 1.0, 'transfers': 0.3, 'accessibility': 0.0}

        # Check if stations exist
        if start_station_id not in self.graph or end_station_id not in self.graph:
            return None

        # Define weight function based on preferences
        def weight_function(from_node, to_node, edge_data):
            time_weight = edge_data['current_travel_time'] * preferences['time']
            transfer_weight = 0
            if 'transfer' in edge_data and edge_data['transfer']:
                transfer_weight = 10 * preferences['transfers']  # Penalty for transfers

            accessibility_weight = 0
            if preferences['accessibility'] > 0:
                # Check if stations are accessible
                from_accessible = self.stations[from_node].get('wheelchair_accessible', 'unknown') == 'yes'
                to_accessible = self.stations[to_node].get('wheelchair_accessible', 'unknown') == 'yes'
                if not from_accessible or not to_accessible:
                    accessibility_weight = 1000 * preferences[
                        'accessibility']  # Big penalty for non-accessible stations

            return time_weight + transfer_weight + accessibility_weight

        # Find shortest path
        try:
            path = nx.shortest_path(
                self.graph,
                source=start_station_id,
                target=end_station_id,
                weight=weight_function
            )

            # Calculate route details
            route_details = []
            total_time = 0
            total_distance = 0

            for i in range(len(path) - 1):
                from_id = path[i]
                to_id = path[i + 1]
                edge_data = self.graph.get_edge_data(from_id, to_id)

                step = {
                    'from_station': self.stations[from_id]['name'],
                    'to_station': self.stations[to_id]['name'],
                    'transport_type': edge_data['transport_type'],
                    'line': edge_data['line'],
                    'travel_time': edge_data['current_travel_time'],
                    'congestion_factor': edge_data['congestion_factor']
                }

                route_details.append(step)
                total_time += edge_data['current_travel_time']

            return {
                'path': path,
                'route_details': route_details,
                'total_time': total_time,
                'num_transfers': len(path) - 2
            }

        except nx.NetworkXNoPath:
            return None
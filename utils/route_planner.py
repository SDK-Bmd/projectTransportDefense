import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class RouteStep:
    transport_type: str
    line: str
    from_station: str
    to_station: str
    departure_time: str
    arrival_time: str
    duration_minutes: int
    distance_km: float = 0.0
    emissions_g: float = 0.0
    platform: str = ""
    direction: str = ""


@dataclass
class Route:
    steps: List[RouteStep]
    total_duration_minutes: int
    total_distance_km: float
    total_emissions_g: float
    num_transfers: int
    accessibility_score: float
    cost_euros: float
    route_type: str  # "fastest", "eco", "accessible", etc.


class RealRoutePlanner:
    """Real route planner using RATP and IDFM APIs"""

    def __init__(self):
        self.idfm_api_key = os.getenv("IDFM_API_KEY")
        self.session = requests.Session()

        # Emission factors (grams CO2 per km per passenger)
        self.emission_factors = {
            'metro': 4,
            'rer': 6,
            'bus': 70,
            'tramway': 3,
            'transilien': 8,
            'walking': 0
        }

        # Cost factors (euros per km)
        self.cost_factors = {
            'metro': 0.15,
            'rer': 0.20,
            'bus': 0.12,
            'tramway': 0.15,
            'transilien': 0.25,
            'walking': 0.0
        }

        # Base API URLs
        self.ratp_base = "https://api-ratp.pierre-grimaud.fr/v4"
        self.idfm_base = "https://prim.iledefrance-mobilites.fr/marketplace"

    def find_station_id(self, station_name: str, stations_df: pd.DataFrame) -> Optional[str]:
        """Find station ID from name using available station data"""
        if stations_df.empty:
            return None

        # Try exact match first
        exact_match = stations_df[stations_df['name'].str.lower() == station_name.lower()]
        if not exact_match.empty:
            return exact_match.iloc[0].get('id', exact_match.iloc[0].get('name'))

        # Try partial match
        partial_match = stations_df[stations_df['name'].str.contains(station_name, case=False, na=False)]
        if not partial_match.empty:
            return partial_match.iloc[0].get('id', partial_match.iloc[0].get('name'))

        return None

    def get_coordinates(self, station_name: str, stations_df: pd.DataFrame) -> Tuple[float, float]:
        """Get station coordinates"""
        if stations_df.empty:
            return 48.8917, 2.2385  # Default La Défense coordinates

        station_match = stations_df[stations_df['name'].str.contains(station_name, case=False, na=False)]
        if not station_match.empty:
            row = station_match.iloc[0]
            lat = float(row.get('lat', 48.8917))
            lon = float(row.get('lon', 2.2385))
            return lat, lon

        return 48.8917, 2.2385

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        from math import radians, cos, sin, asin, sqrt

        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers

        return c * r

    def call_idfm_journey_planner(self, origin: str, destination: str,
                                  departure_time: datetime = None) -> Optional[Dict]:
        """Call IDFM journey planner API"""
        if not self.idfm_api_key:
            logger.warning("No IDFM API key available")
            return None

        if not departure_time:
            departure_time = datetime.now()

        url = f"{self.idfm_base}/journey-planner"
        headers = {"Authorization": self.idfm_api_key}

        params = {
            "from": origin,
            "to": destination,
            "datetime": departure_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "modes": "metro,rer,bus,tramway,transilien",
            "maxResults": 5
        }

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"IDFM API returned status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error calling IDFM API: {e}")
            return None

    def call_ratp_schedules(self, transport_type: str, line: str, station: str) -> Optional[Dict]:
        """Get real-time schedules from RATP API"""
        url = f"{self.ratp_base}/schedules/{transport_type}/{line}/{station}/A+R"

        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"RATP schedules API returned status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error calling RATP schedules API: {e}")
            return None

    def build_routes_from_gtfs(self, origin: str, destination: str,
                               stations_df: pd.DataFrame, schedules_df: pd.DataFrame) -> List[Route]:
        """Build routes using available GTFS and schedule data"""
        routes = []

        # Get coordinates for distance calculation
        origin_lat, origin_lon = self.get_coordinates(origin, stations_df)
        dest_lat, dest_lon = self.get_coordinates(destination, stations_df)
        total_distance = self.calculate_distance(origin_lat, origin_lon, dest_lat, dest_lon)

        # If we have schedule data, use it to build realistic routes
        if not schedules_df.empty:
            # Group by transport type and line
            for transport_type in schedules_df.get('transport_type', pd.Series()).unique():
                if pd.isna(transport_type):
                    continue

                type_schedules = schedules_df[schedules_df['transport_type'] == transport_type]

                for line in type_schedules.get('line', pd.Series()).unique():
                    if pd.isna(line):
                        continue

                    # Build route for this line
                    line_schedules = type_schedules[type_schedules['line'] == line]

                    # Calculate realistic travel time based on distance and transport type
                    if transport_type.lower() in ['metro', 'rer', 'rers']:
                        avg_speed_kmh = 35
                    elif transport_type.lower() in ['bus', 'buses']:
                        avg_speed_kmh = 20
                    elif transport_type.lower() == 'tramway':
                        avg_speed_kmh = 25
                    else:
                        avg_speed_kmh = 30

                    travel_time = max(5, (total_distance / avg_speed_kmh) * 60)  # Convert to minutes

                    # Create route step
                    now = datetime.now()
                    step = RouteStep(
                        transport_type=transport_type,
                        line=str(line),
                        from_station=origin,
                        to_station=destination,
                        departure_time=now.strftime("%H:%M"),
                        arrival_time=(now + timedelta(minutes=travel_time)).strftime("%H:%M"),
                        duration_minutes=int(travel_time),
                        distance_km=total_distance,
                        emissions_g=total_distance * self.emission_factors.get(transport_type.lower(), 50),
                        direction=line_schedules.get('direction', pd.Series()).iloc[
                            0] if not line_schedules.empty else ""
                    )

                    # Create route
                    route = Route(
                        steps=[step],
                        total_duration_minutes=int(travel_time),
                        total_distance_km=total_distance,
                        total_emissions_g=step.emissions_g,
                        num_transfers=0,
                        accessibility_score=0.9 if transport_type.lower() in ['metro', 'rer'] else 0.7,
                        cost_euros=total_distance * self.cost_factors.get(transport_type.lower(), 0.15),
                        route_type=f"{transport_type}_{line}"
                    )

                    routes.append(route)

        # Add walking route if distance is reasonable
        if total_distance <= 3.0:  # Within 3km
            walking_time = total_distance * 12  # 12 minutes per km
            walking_step = RouteStep(
                transport_type="walking",
                line="",
                from_station=origin,
                to_station=destination,
                departure_time=datetime.now().strftime("%H:%M"),
                arrival_time=(datetime.now() + timedelta(minutes=walking_time)).strftime("%H:%M"),
                duration_minutes=int(walking_time),
                distance_km=total_distance,
                emissions_g=0,
                direction=""
            )

            walking_route = Route(
                steps=[walking_step],
                total_duration_minutes=int(walking_time),
                total_distance_km=total_distance,
                total_emissions_g=0,
                num_transfers=0,
                accessibility_score=1.0,
                cost_euros=0.0,
                route_type="walking"
            )

            routes.append(walking_route)

        return routes

    def plan_routes(self, origin: str, destination: str, preferences: Dict,
                    transport_modes: List[str], stations_df: pd.DataFrame,
                    schedules_df: pd.DataFrame, departure_time: datetime = None) -> List[Route]:
        """Main route planning function"""

        routes = []

        # Try IDFM API first (most comprehensive)
        if self.idfm_api_key:
            logger.info("Attempting IDFM route planning...")
            idfm_routes = self.call_idfm_journey_planner(origin, destination, departure_time)

            if idfm_routes:
                # Parse IDFM response into Route objects
                routes.extend(self.parse_idfm_routes(idfm_routes, preferences))
                logger.info(f"Found {len(routes)} routes from IDFM")

        # If no IDFM routes or as fallback, use local data
        if not routes:
            logger.info("Using local GTFS/schedule data for route planning...")
            routes = self.build_routes_from_gtfs(origin, destination, stations_df, schedules_df)

        # Filter routes by selected transport modes
        filtered_routes = []
        for route in routes:
            route_has_allowed_mode = False
            for step in route.steps:
                step_type = step.transport_type.lower()
                if (("Metro" in transport_modes and step_type in ['metro']) or
                        ("RER" in transport_modes and step_type in ['rer', 'rers']) or
                        ("Bus" in transport_modes and step_type in ['bus', 'buses']) or
                        ("Transilien" in transport_modes and step_type in ['transilien']) or
                        ("Walking" in transport_modes and step_type == 'walking')):
                    route_has_allowed_mode = True
                    break

            if route_has_allowed_mode:
                filtered_routes.append(route)

        # Apply user preferences to sort routes
        filtered_routes = self.apply_preferences(filtered_routes, preferences)

        return filtered_routes[:5]  # Return top 5 routes

    def parse_idfm_routes(self, idfm_data: Dict, preferences: Dict) -> List[Route]:
        """Parse IDFM API response into Route objects"""
        routes = []

        journeys = idfm_data.get('journeys', [])

        for journey in journeys:
            steps = []
            total_duration = 0
            total_distance = 0
            total_emissions = 0

            sections = journey.get('sections', [])

            for section in sections:
                if section.get('type') == 'public_transport':
                    # Public transport section
                    from_stop = section.get('from', {}).get('name', '')
                    to_stop = section.get('to', {}).get('name', '')
                    duration = section.get('duration', 0) // 60  # Convert to minutes

                    display_info = section.get('display_informations', {})
                    transport_type = display_info.get('physical_mode', '').lower()
                    line = display_info.get('code', '')
                    direction = display_info.get('direction', '')

                    step = RouteStep(
                        transport_type=transport_type,
                        line=line,
                        from_station=from_stop,
                        to_station=to_stop,
                        departure_time=section.get('departure_date_time', ''),
                        arrival_time=section.get('arrival_date_time', ''),
                        duration_minutes=duration,
                        direction=direction
                    )

                    steps.append(step)
                    total_duration += duration

                elif section.get('type') == 'street_network':
                    # Walking section
                    if section.get('mode') == 'walking':
                        duration = section.get('duration', 0) // 60
                        distance = section.get('geojson', {}).get('properties', {}).get('length', 0) / 1000

                        step = RouteStep(
                            transport_type='walking',
                            line='',
                            from_station=section.get('from', {}).get('name', ''),
                            to_station=section.get('to', {}).get('name', ''),
                            departure_time='',
                            arrival_time='',
                            duration_minutes=duration,
                            distance_km=distance
                        )

                        steps.append(step)
                        total_duration += duration
                        total_distance += distance

            # Calculate total emissions and cost
            for step in steps:
                emission_factor = self.emission_factors.get(step.transport_type, 0)
                step.emissions_g = step.distance_km * emission_factor
                total_emissions += step.emissions_g

            # Count transfers
            transport_steps = [s for s in steps if s.transport_type != 'walking']
            num_transfers = max(0, len(transport_steps) - 1)

            route = Route(
                steps=steps,
                total_duration_minutes=total_duration,
                total_distance_km=total_distance,
                total_emissions_g=total_emissions,
                num_transfers=num_transfers,
                accessibility_score=0.8,  # Default value
                cost_euros=total_distance * 0.15,  # Approximate
                route_type="idfm_route"
            )

            routes.append(route)

        return routes

    def apply_preferences(self, routes: List[Route], preferences: Dict) -> List[Route]:
        """Sort routes based on user preferences"""

        def route_score(route: Route) -> float:
            score = 0

            # Time preference (lower duration = higher score)
            if route.total_duration_minutes > 0:
                time_score = 100 / route.total_duration_minutes
                score += time_score * preferences.get('time_pref', 1.0)

            # Transfer preference (fewer transfers = higher score)
            transfer_score = 10 / max(1, route.num_transfers)
            score += transfer_score * preferences.get('transfer_pref', 0.3)

            # Environmental preference (lower emissions = higher score)
            if route.total_emissions_g > 0:
                eco_score = 1000 / route.total_emissions_g
                score += eco_score * preferences.get('eco_pref', 0.2)
            else:
                score += 50 * preferences.get('eco_pref', 0.2)  # Bonus for zero emissions

            # Cost preference (lower cost = higher score)
            if route.cost_euros > 0:
                cost_score = 10 / route.cost_euros
                score += cost_score * preferences.get('cost_pref', 0.2)

            # Accessibility preference
            score += route.accessibility_score * 20 * preferences.get('accessibility_pref', 0.1)

            return score

        return sorted(routes, key=route_score, reverse=True)


# Updated calculate_routes function to use the real planner
def calculate_routes_real(origin: str, destination: str, preferences: Dict,
                          transport_modes: List[str], stations_df: pd.DataFrame,
                          schedules_df: pd.DataFrame, traffic_df: pd.DataFrame) -> Dict:
    """Calculate real routes using RATP/IDFM APIs and real data"""

    planner = RealRoutePlanner()

    try:
        routes = planner.plan_routes(
            origin=origin,
            destination=destination,
            preferences=preferences,
            transport_modes=transport_modes,
            stations_df=stations_df,
            schedules_df=schedules_df
        )

        if not routes:
            return {"No routes found": {"message": "No routes available for the selected criteria"}}

        # Convert Route objects to the format expected by the UI
        result_routes = {}

        for i, route in enumerate(routes):
            # Determine route name based on primary transport mode
            primary_mode = route.steps[0].transport_type if route.steps else "Mixed"
            route_icons = {
                'metro': '🚇',
                'rer': '🚄',
                'rers': '🚄',
                'bus': '🚌',
                'buses': '🚌',
                'tramway': '🚊',
                'transilien': '🚂',
                'walking': '🚶'
            }

            icon = route_icons.get(primary_mode.lower(), '🚊')

            if route.route_type == "walking":
                route_name = f"{icon} Walking Route"
            elif len([s for s in route.steps if s.transport_type != 'walking']) == 1:
                # Single transport mode
                main_step = next(s for s in route.steps if s.transport_type != 'walking')
                route_name = f"{icon} {main_step.transport_type.title()} {main_step.line}"
            else:
                # Multi-modal
                route_name = f"{icon} Multi-modal Route"

            # Convert RouteStep objects to dictionaries
            route_details = []
            for step in route.steps:
                route_details.append({
                    "transport_type": step.transport_type,
                    "line": step.line,
                    "from_station": step.from_station,
                    "to_station": step.to_station,
                    "travel_time": step.duration_minutes,
                    "congestion_factor": 1.0,  # Can be enhanced with traffic data
                    "emissions_g": step.emissions_g,
                    "departure_time": step.departure_time,
                    "arrival_time": step.arrival_time,
                    "direction": step.direction
                })

            result_routes[route_name] = {
                "route_details": route_details,
                "total_time": route.total_duration_minutes,
                "num_transfers": route.num_transfers,
                "total_emissions": int(route.total_emissions_g),
                "accessibility_score": route.accessibility_score,
                "cost_euros": route.cost_euros
            }

        return result_routes

    except Exception as e:
        logger.error(f"Error in route planning: {e}")
        return {"Error": {"message": f"Route planning failed: {str(e)}"}}
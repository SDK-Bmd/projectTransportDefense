import requests
import json
import hashlib
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, asdict
import streamlit as st

# Import the caching system
from utils.route_cache import RouteCache, CacheConfig
from utils.data_lake_utils import get_s3_client, save_json_to_data_lake, read_json_from_data_lake

logger = logging.getLogger(__name__)


class EnhancedCachedRoutePlanner:
    """Enhanced route planner with both proper time handling AND intelligent caching"""

    def __init__(self, cache_config: CacheConfig = None):
        self.cache_config = cache_config or CacheConfig(
            routes_ttl_minutes=60,
            api_responses_ttl_minutes=10,
            schedules_ttl_minutes=15
        )
        self.cache = RouteCache(self.cache_config)

        # Emission factors (grams CO2 per km per passenger)
        self.emission_factors = {
            'metro': 4, 'rer': 6, 'bus': 70, 'tramway': 3, 'transilien': 8, 'walking': 0
        }

        # Cost factors (euros per journey)
        self.cost_factors = {
            'metro': 1.90, 'rer': 1.90, 'bus': 1.90, 'tramway': 1.90, 'transilien': 3.65, 'walking': 0.0
        }

    def _generate_cache_key_with_time(self, origin: str, destination: str, preferences: Dict,
                                      transport_modes: List[str], departure_time: time = None) -> str:
        """Generate cache key that includes departure time"""

        # Round departure time to nearest 15 minutes for better cache hits
        if departure_time:
            # Round to nearest 15-minute interval for cache efficiency
            minutes = departure_time.minute
            rounded_minutes = (minutes // 15) * 15
            rounded_time = departure_time.replace(minute=rounded_minutes, second=0, microsecond=0)
            time_str = rounded_time.strftime("%H:%M")
        else:
            time_str = "now"

        cache_data = {
            'origin': origin,
            'destination': destination,
            'preferences': preferences,
            'transport_modes': sorted(transport_modes),  # Sort for consistent hashing
            'departure_time': time_str,
            'date': datetime.now().strftime('%Y-%m-%d')  # Include date for daily cache refresh
        }

        # Create consistent hash
        sorted_data = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(sorted_data.encode()).hexdigest()

    def get_cached_route_with_time(self, origin: str, destination: str, preferences: Dict,
                                   transport_modes: List[str], departure_time: time = None) -> Optional[Dict]:
        """Get cached route that includes departure time consideration"""

        cache_key = self._generate_cache_key_with_time(origin, destination, preferences, transport_modes,
                                                       departure_time)

        # Check memory cache first
        cached = self.cache._get_from_memory_cache(cache_key)
        if cached:
            logger.info(f"Memory cache hit for timed route: {origin} -> {destination} at {departure_time}")
            return cached['routes']

        # Check data lake cache
        bucket_name = self.cache.bucket_name
        s3_key = f"{self.cache.cache_prefixes['routes']}{cache_key}.json"
        cached_data = read_json_from_data_lake(bucket_name, s3_key)

        if cached_data:
            if self.cache._is_cache_valid(cached_data['timestamp'], self.cache_config.routes_ttl_minutes):
                # Add back to memory cache
                self.cache._set_memory_cache(cache_key, cached_data, self.cache_config.routes_ttl_minutes)
                logger.info(f"Data lake cache hit for timed route: {origin} -> {destination}")
                return cached_data['routes']
            else:
                logger.debug(f"Cache expired for timed route: {origin} -> {destination}")

        return None

    def cache_route_with_time(self, origin: str, destination: str, preferences: Dict,
                              transport_modes: List[str], routes: Dict, departure_time: time = None):
        """Cache route result with time consideration"""

        cache_key = self._generate_cache_key_with_time(origin, destination, preferences, transport_modes,
                                                       departure_time)

        cache_entry = {
            'timestamp': datetime.now().isoformat(),
            'cache_key': cache_key,
            'query': {
                'origin': origin,
                'destination': destination,
                'preferences': preferences,
                'transport_modes': transport_modes,
                'departure_time': departure_time.strftime("%H:%M") if departure_time else "now",
                'date': datetime.now().strftime('%Y-%m-%d')
            },
            'routes': routes,
            'ttl_minutes': self.cache_config.routes_ttl_minutes
        }

        # Save to data lake
        bucket_name = self.cache.bucket_name
        s3_key = f"{self.cache.cache_prefixes['routes']}{cache_key}.json"
        save_json_to_data_lake(bucket_name, s3_key, cache_entry)

        # Also save to memory cache
        self.cache._set_memory_cache(cache_key, cache_entry, self.cache_config.routes_ttl_minutes)

        time_str = departure_time.strftime("%H:%M") if departure_time else "now"
        logger.info(f"Cached timed route result: {origin} -> {destination} at {time_str}")

    def plan_routes_cached_with_time(self, origin: str, destination: str, preferences: Dict,
                                     transport_modes: List[str], stations_df: pd.DataFrame,
                                     schedules_df: pd.DataFrame, traffic_df: pd.DataFrame,
                                     departure_time: time = None) -> Dict:
        """Main route planning function with time handling and caching"""

        # Track popularity for cache optimization
        self.cache.track_popular_routes(origin, destination)

        # Try to get from cache first
        cached_routes = self.get_cached_route_with_time(origin, destination, preferences, transport_modes,
                                                        departure_time)
        if cached_routes:
            time_str = departure_time.strftime("%H:%M") if departure_time else "now"
            logger.info(f"Using cached timed route: {origin} -> {destination} at {time_str}")
            return cached_routes

        # Cache miss - calculate new routes with proper time handling
        time_str = departure_time.strftime("%H:%M") if departure_time else "now"
        logger.info(f"Calculating new timed route: {origin} -> {destination} at {time_str}")

        try:
            routes = self._calculate_routes_with_time(
                origin=origin,
                destination=destination,
                preferences=preferences,
                transport_modes=transport_modes,
                stations_df=stations_df,
                schedules_df=schedules_df,
                traffic_df=traffic_df,
                departure_time=departure_time
            )

            if routes and "Error" not in routes and "No routes found" not in routes:
                # Cache the successful result
                self.cache_route_with_time(origin, destination, preferences, transport_modes, routes, departure_time)

            return routes

        except Exception as e:
            logger.error(f"Error in cached timed route planning: {e}")
            return {"Error": {"message": f"Route planning failed: {str(e)}"}}

    def _calculate_routes_with_time(self, origin: str, destination: str, preferences: Dict,
                                    transport_modes: List[str], stations_df: pd.DataFrame,
                                    schedules_df: pd.DataFrame, traffic_df: pd.DataFrame,
                                    departure_time: time = None) -> Dict:
        """Calculate routes with proper time handling (the fixed version)"""

        # Convert departure time to datetime for calculations
        if departure_time:
            departure_datetime = datetime.combine(datetime.today(), departure_time)
            departure_str = departure_time.strftime("%H:%M")
        else:
            departure_datetime = datetime.now()
            departure_str = departure_datetime.strftime("%H:%M")

        # Validate inputs
        if origin == destination:
            return {"Error": {"message": "Origin and destination cannot be the same"}}

        if not transport_modes:
            return {"Error": {"message": "Please select at least one transport mode"}}

        # Get station coordinates for distance calculation
        origin_coords = self._get_station_coordinates(origin, stations_df)
        dest_coords = self._get_station_coordinates(destination, stations_df)

        # Calculate distance
        if origin_coords and dest_coords:
            distance_km = self._calculate_haversine_distance(origin_coords, dest_coords)
        else:
            # Default distance for La Défense area routes
            distance_km = 2.5

        routes = {}

        # Generate routes for each selected transport mode with TIME AWARENESS
        if "Metro" in transport_modes:
            metro_route = self._generate_metro_route_with_time(origin, destination, departure_datetime, distance_km,
                                                               traffic_df)
            if metro_route:
                routes.update(metro_route)

        if "RER" in transport_modes:
            rer_route = self._generate_rer_route_with_time(origin, destination, departure_datetime, distance_km,
                                                           traffic_df)
            if rer_route:
                routes.update(rer_route)

        if "Bus" in transport_modes:
            bus_route = self._generate_bus_route_with_time(origin, destination, departure_datetime, distance_km,
                                                           traffic_df)
            if bus_route:
                routes.update(bus_route)

        if "Transilien" in transport_modes:
            transilien_route = self._generate_transilien_route_with_time(origin, destination, departure_datetime,
                                                                         distance_km)
            if transilien_route:
                routes.update(transilien_route)

        if "Walking" in transport_modes and distance_km <= 4.0:
            walking_route = self._generate_walking_route_with_time(origin, destination, departure_datetime, distance_km)
            if walking_route:
                routes.update(walking_route)

        # Apply preferences to sort routes
        if routes:
            routes = self._apply_route_preferences(routes, preferences)
            return routes
        else:
            return {"No routes found": {
                "message": f"No routes available for selected transport modes",
                "debug_info": {
                    "departure_time": departure_str,
                    "distance_km": distance_km,
                    "transport_modes": transport_modes
                }
            }}

    def _get_station_coordinates(self, station_name: str, stations_df: pd.DataFrame) -> Optional[Tuple[float, float]]:
        """Get coordinates for a station"""
        if stations_df.empty:
            return None

        # Try exact match first
        exact_match = stations_df[stations_df['name'].str.lower() == station_name.lower()]
        if not exact_match.empty:
            row = exact_match.iloc[0]
            lat, lon = float(row.get('lat', 0)), float(row.get('lon', 0))
            if lat != 0 and lon != 0:
                return (lat, lon)

        # Try partial match
        partial_match = stations_df[stations_df['name'].str.contains(station_name, case=False, na=False)]
        if not partial_match.empty:
            row = partial_match.iloc[0]
            lat, lon = float(row.get('lat', 0)), float(row.get('lon', 0))
            if lat != 0 and lon != 0:
                return (lat, lon)

        return None

    def _calculate_haversine_distance(self, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """Calculate distance between two coordinates"""
        from math import radians, cos, sin, asin, sqrt

        lat1, lon1 = coord1
        lat2, lon2 = coord2

        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers

        return c * r

    def _generate_metro_route_with_time(self, origin: str, destination: str, departure_datetime: datetime,
                                        distance_km: float, traffic_df: pd.DataFrame) -> Dict:
        """Generate Metro route with TIME-AWARE calculations"""

        base_time = max(8, min(25, distance_km * 2.5 + 5))

        # RUSH HOUR CALCULATIONS - This is what was missing!
        hour = departure_datetime.hour
        if 7 <= hour <= 9:  # Morning rush
            congestion_factor = 1.3
            frequency = "2-3 min"
            rush_note = "Morning rush hour"
        elif 17 <= hour <= 19:  # Evening rush
            congestion_factor = 1.4
            frequency = "2-3 min"
            rush_note = "Evening rush hour"
        else:
            congestion_factor = 1.0
            frequency = "4-6 min"
            rush_note = "Normal service"

        # Check for service disruptions
        if not traffic_df.empty:
            metro_issues = traffic_df[traffic_df.get('transport_type', '').str.lower() == 'metro']
            if not metro_issues.empty and any(metro_issues.get('status', '') != 'normal'):
                congestion_factor *= 1.2  # Additional delay for disruptions

        travel_time = int(base_time * congestion_factor)
        arrival_time = departure_datetime + timedelta(minutes=travel_time)

        route_details = [{
            "transport_type": "metro",
            "line": "1",
            "from_station": origin,
            "to_station": destination,
            "travel_time": travel_time,
            "congestion_factor": congestion_factor,
            "emissions_g": distance_km * self.emission_factors['metro'],
            "departure_time": departure_datetime.strftime("%H:%M"),
            "arrival_time": arrival_time.strftime("%H:%M"),
            "direction": "Direction Château de Vincennes",
            "frequency": frequency,
            "platform": "Platform 1",
            "rush_hour_note": rush_note
        }]

        return {
            "🚇 Metro Line 1": {
                "route_details": route_details,
                "total_time": travel_time,
                "num_transfers": 0,
                "total_emissions": int(distance_km * self.emission_factors['metro']),
                "accessibility_score": 0.95,
                "cost_euros": self.cost_factors['metro'],
                "departure_time": departure_datetime.strftime("%H:%M"),
                "arrival_time": arrival_time.strftime("%H:%M"),
                "rush_hour_factor": congestion_factor
            }
        }

    def _generate_rer_route_with_time(self, origin: str, destination: str, departure_datetime: datetime,
                                      distance_km: float, traffic_df: pd.DataFrame) -> Dict:
        """Generate RER route with TIME-AWARE calculations"""

        base_time = max(10, min(30, distance_km * 2.0 + 8))

        # RER rush hour impact
        hour = departure_datetime.hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            congestion_factor = 1.2
            frequency = "3-4 min"
        else:
            congestion_factor = 1.0
            frequency = "6-8 min"

        travel_time = int(base_time * congestion_factor)
        arrival_time = departure_datetime + timedelta(minutes=travel_time)

        route_details = [{
            "transport_type": "rer",
            "line": "A",
            "from_station": origin,
            "to_station": destination,
            "travel_time": travel_time,
            "congestion_factor": congestion_factor,
            "emissions_g": distance_km * self.emission_factors['rer'],
            "departure_time": departure_datetime.strftime("%H:%M"),
            "arrival_time": arrival_time.strftime("%H:%M"),
            "direction": "Direction Marne-la-Vallée",
            "frequency": frequency,
            "platform": "Platform A"
        }]

        return {
            "🚄 RER A": {
                "route_details": route_details,
                "total_time": travel_time,
                "num_transfers": 0,
                "total_emissions": int(distance_km * self.emission_factors['rer']),
                "accessibility_score": 0.90,
                "cost_euros": self.cost_factors['rer'],
                "departure_time": departure_datetime.strftime("%H:%M"),
                "arrival_time": arrival_time.strftime("%H:%M"),
                "rush_hour_factor": congestion_factor
            }
        }

    def _generate_bus_route_with_time(self, origin: str, destination: str, departure_datetime: datetime,
                                      distance_km: float, traffic_df: pd.DataFrame) -> Dict:
        """Generate Bus route with HEAVY time-dependent traffic impact"""

        base_time = max(12, min(40, distance_km * 4.0 + 10))

        # Buses are HEAVILY affected by rush hour - this is key!
        hour = departure_datetime.hour
        if 7 <= hour <= 9:  # Morning rush - WORST for buses
            congestion_factor = 1.8  # 80% longer!
            frequency = "8-10 min"
            traffic_note = "Heavy traffic expected"
        elif 17 <= hour <= 19:  # Evening rush
            congestion_factor = 1.6
            frequency = "6-8 min"
            traffic_note = "Moderate traffic"
        else:
            congestion_factor = 1.1
            frequency = "10-15 min"
            traffic_note = "Normal traffic"

        travel_time = int(base_time * congestion_factor)
        arrival_time = departure_datetime + timedelta(minutes=travel_time)

        route_details = [{
            "transport_type": "bus",
            "line": "144",
            "from_station": origin,
            "to_station": destination,
            "travel_time": travel_time,
            "congestion_factor": congestion_factor,
            "emissions_g": distance_km * self.emission_factors['bus'],
            "departure_time": departure_datetime.strftime("%H:%M"),
            "arrival_time": arrival_time.strftime("%H:%M"),
            "direction": "Direction Pont de Neuilly",
            "frequency": frequency,
            "traffic_note": traffic_note
        }]

        return {
            "🚌 Bus 144": {
                "route_details": route_details,
                "total_time": travel_time,
                "num_transfers": 0,
                "total_emissions": int(distance_km * self.emission_factors['bus']),
                "accessibility_score": 0.75,
                "cost_euros": self.cost_factors['bus'],
                "departure_time": departure_datetime.strftime("%H:%M"),
                "arrival_time": arrival_time.strftime("%H:%M"),
                "rush_hour_factor": congestion_factor,
                "traffic_impact": traffic_note
            }
        }

    def _generate_transilien_route_with_time(self, origin: str, destination: str, departure_datetime: datetime,
                                             distance_km: float) -> Dict:
        """Generate Transilien route"""

        base_time = max(15, min(35, distance_km * 3.0 + 12))
        travel_time = int(base_time * 1.1)
        arrival_time = departure_datetime + timedelta(minutes=travel_time)

        route_details = [{
            "transport_type": "transilien",
            "line": "L",
            "from_station": origin,
            "to_station": destination,
            "travel_time": travel_time,
            "congestion_factor": 1.1,
            "emissions_g": distance_km * self.emission_factors['transilien'],
            "departure_time": departure_datetime.strftime("%H:%M"),
            "arrival_time": arrival_time.strftime("%H:%M"),
            "direction": "Direction La Défense",
            "frequency": "15-20 min"
        }]

        return {
            "🚂 Transilien L": {
                "route_details": route_details,
                "total_time": travel_time,
                "num_transfers": 0,
                "total_emissions": int(distance_km * self.emission_factors['transilien']),
                "accessibility_score": 0.85,
                "cost_euros": self.cost_factors['transilien'],
                "departure_time": departure_datetime.strftime("%H:%M"),
                "arrival_time": arrival_time.strftime("%H:%M")
            }
        }

    def _generate_walking_route_with_time(self, origin: str, destination: str, departure_datetime: datetime,
                                          distance_km: float) -> Dict:
        """Generate walking route"""

        walking_time = int(distance_km * 12)  # 12 minutes per km

        if walking_time > 60:  # Don't suggest walks over 1 hour
            return {}

        arrival_time = departure_datetime + timedelta(minutes=walking_time)

        route_details = [{
            "transport_type": "walking",
            "line": "",
            "from_station": origin,
            "to_station": destination,
            "travel_time": walking_time,
            "congestion_factor": 1.0,
            "emissions_g": 0,
            "departure_time": departure_datetime.strftime("%H:%M"),
            "arrival_time": arrival_time.strftime("%H:%M"),
            "direction": f"Walk directly to {destination}",
            "distance": f"{distance_km:.1f} km"
        }]

        return {
            "🚶 Walking": {
                "route_details": route_details,
                "total_time": walking_time,
                "num_transfers": 0,
                "total_emissions": 0,
                "accessibility_score": 1.0,
                "cost_euros": 0.0,
                "departure_time": departure_datetime.strftime("%H:%M"),
                "arrival_time": arrival_time.strftime("%H:%M"),
                "health_benefit": f"Burns ~{int(walking_time * 1.5)} calories"
            }
        }

    def _apply_route_preferences(self, routes: Dict, preferences: Dict) -> Dict:
        """Sort routes based on user preferences"""

        def calculate_route_score(route_data: Dict) -> float:
            score = 0

            # Time preference
            time_factor = 1.0 / max(1, route_data.get('total_time', 30))
            score += time_factor * 100 * preferences.get('time_pref', 1.0)

            # Transfer preference
            transfer_factor = 1.0 / max(1, route_data.get('num_transfers', 0) + 1)
            score += transfer_factor * 50 * preferences.get('transfer_pref', 0.3)

            # Environmental preference
            emission_factor = 1.0 / max(1, route_data.get('total_emissions', 50))
            score += emission_factor * 1000 * preferences.get('eco_pref', 0.2)

            # Cost preference
            cost_factor = 1.0 / max(0.1, route_data.get('cost_euros', 2.0))
            score += cost_factor * 10 * preferences.get('cost_pref', 0.2)

            # Accessibility preference
            accessibility_factor = route_data.get('accessibility_score', 0.8)
            score += accessibility_factor * 20 * preferences.get('accessibility_pref', 0.1)

            return score

        return dict(sorted(routes.items(), key=lambda x: calculate_route_score(x[1]), reverse=True))


@st.cache_resource
def get_enhanced_cached_route_planner():
    """Initialize enhanced cached route planner (cached across sessions)"""
    cache_config = CacheConfig(
        routes_ttl_minutes=60,  # Cache routes for 1 hour
        api_responses_ttl_minutes=10,  # Cache API responses for 10 minutes
        schedules_ttl_minutes=15  # Cache schedules for 15 minutes
    )
    return EnhancedCachedRoutePlanner(cache_config)


def calculate_routes_cached_with_time_fix(origin: str, destination: str, preferences: dict,
                                          transport_modes: list, stations_df, schedules_df, traffic_df,
                                          departure_time: time = None):
    """
    Drop-in replacement for your route calculation that includes BOTH caching AND time fixing
    """
    planner = get_enhanced_cached_route_planner()

    return planner.plan_routes_cached_with_time(
        origin=origin,
        destination=destination,
        preferences=preferences,
        transport_modes=transport_modes,
        stations_df=stations_df,
        schedules_df=schedules_df,
        traffic_df=traffic_df,
        departure_time=departure_time  # ← This is the key fix!
    )


# Enhanced cache management for sidebar
def add_enhanced_cache_management_sidebar():
    """Add enhanced cache management controls to sidebar"""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🗄️ Smart Cache System")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🧹 Clear Cache", help="Remove expired cache entries"):
                planner = get_enhanced_cached_route_planner()
                planner.cache.cleanup_expired_cache()
                st.success("Cache cleaned!")
                st.rerun()

        with col2:
            if st.button("📊 Cache Stats", help="Show cache usage statistics"):
                planner = get_enhanced_cached_route_planner()
                stats = planner.cache.get_cache_stats()

                st.info(f"**Cached Routes:** {stats['total_cached_items']}")
                st.info(f"**Memory Cache:** {stats['memory_cache_items']}")

                with st.expander("Detailed Cache Stats"):
                    for cache_type, type_stats in stats['cache_types'].items():
                        if 'error' not in type_stats:
                            st.write(f"**{cache_type.title()}:**")
                            st.write(f"- Valid: {type_stats['valid_files']}")
                            st.write(f"- Expired: {type_stats['expired_files']}")
                            st.write(f"- TTL: {type_stats['ttl_minutes']}min")
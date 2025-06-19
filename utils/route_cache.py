import json
import hashlib
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import logging
from dataclasses import dataclass, asdict
import os
import sys

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_lake_utils import (
    get_s3_client, save_json_to_data_lake, read_json_from_data_lake,
    save_parquet_to_data_lake, read_parquet_from_data_lake
)
from configuration.config import DATA_LAKE

logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Configuration for different types of cache"""
    routes_ttl_minutes: int = 60  # Route results cache for 1 hour
    stations_ttl_minutes: int = 1440  # Station data cache for 24 hours
    schedules_ttl_minutes: int = 15  # Real-time schedules cache for 15 minutes
    popular_routes_ttl_minutes: int = 30  # Popular routes cache for 30 minutes
    api_responses_ttl_minutes: int = 10  # Raw API responses cache for 10 minutes


class RouteCache:
    """Intelligent caching system for route planning data"""

    def __init__(self, config: CacheConfig = None):
        self.config = config or CacheConfig()
        self.bucket_name = DATA_LAKE["bucket_name"]
        self.s3_client = get_s3_client()

        # Cache prefixes in data lake
        self.cache_prefixes = {
            'routes': 'cache/routes/',
            'stations': 'cache/stations/',
            'schedules': 'cache/schedules/',
            'api_responses': 'cache/api_responses/',
            'popular_routes': 'cache/popular_routes/',
            'travel_times': 'cache/travel_times/'
        }

        # In-memory cache for very frequent requests
        self.memory_cache = {}
        self.memory_cache_expiry = {}

    def _generate_cache_key(self, data: Dict) -> str:
        """Generate consistent cache key from data"""
        # Sort keys to ensure consistent hashing
        sorted_data = json.dumps(data, sort_keys=True)
        return hashlib.md5(sorted_data.encode()).hexdigest()

    def _is_cache_valid(self, cache_timestamp: str, ttl_minutes: int) -> bool:
        """Check if cached data is still valid"""
        try:
            cache_time = datetime.fromisoformat(cache_timestamp)
            expiry_time = cache_time + timedelta(minutes=ttl_minutes)
            return datetime.now() < expiry_time
        except:
            return False

    def _get_from_memory_cache(self, key: str) -> Optional[Dict]:
        """Get data from in-memory cache"""
        if key in self.memory_cache and key in self.memory_cache_expiry:
            if datetime.now() < self.memory_cache_expiry[key]:
                logger.debug(f"Memory cache hit for key: {key[:10]}...")
                return self.memory_cache[key]
            else:
                # Expired, remove from memory cache
                del self.memory_cache[key]
                del self.memory_cache_expiry[key]
        return None

    def _set_memory_cache(self, key: str, data: Dict, ttl_minutes: int):
        """Set data in memory cache"""
        self.memory_cache[key] = data
        self.memory_cache_expiry[key] = datetime.now() + timedelta(minutes=ttl_minutes)

        # Limit memory cache size (keep only 100 most recent items)
        if len(self.memory_cache) > 100:
            # Remove oldest item
            oldest_key = min(self.memory_cache_expiry.keys(),
                             key=lambda k: self.memory_cache_expiry[k])
            del self.memory_cache[oldest_key]
            del self.memory_cache_expiry[oldest_key]

    def cache_route_result(self, origin: str, destination: str, preferences: Dict,
                           transport_modes: List[str], routes: Dict):
        """Cache route planning results"""
        cache_data = {
            'origin': origin,
            'destination': destination,
            'preferences': preferences,
            'transport_modes': transport_modes
        }

        cache_key = self._generate_cache_key(cache_data)

        cache_entry = {
            'timestamp': datetime.now().isoformat(),
            'cache_key': cache_key,
            'query': cache_data,
            'routes': routes,
            'ttl_minutes': self.config.routes_ttl_minutes
        }

        # Save to data lake
        s3_key = f"{self.cache_prefixes['routes']}{cache_key}.json"
        save_json_to_data_lake(self.bucket_name, s3_key, cache_entry)

        # Also save to memory cache for immediate reuse
        self._set_memory_cache(cache_key, cache_entry, self.config.routes_ttl_minutes)

        logger.info(f"Cached route result: {origin} -> {destination}")

    def get_cached_route(self, origin: str, destination: str, preferences: Dict,
                         transport_modes: List[str]) -> Optional[Dict]:
        """Get cached route if available and valid"""
        cache_data = {
            'origin': origin,
            'destination': destination,
            'preferences': preferences,
            'transport_modes': transport_modes
        }

        cache_key = self._generate_cache_key(cache_data)

        # Check memory cache first
        cached = self._get_from_memory_cache(cache_key)
        if cached:
            return cached['routes']

        # Check data lake cache
        s3_key = f"{self.cache_prefixes['routes']}{cache_key}.json"
        cached_data = read_json_from_data_lake(self.bucket_name, s3_key)

        if cached_data:
            if self._is_cache_valid(cached_data['timestamp'], self.config.routes_ttl_minutes):
                # Add back to memory cache
                self._set_memory_cache(cache_key, cached_data, self.config.routes_ttl_minutes)
                logger.info(f"Cache hit for route: {origin} -> {destination}")
                return cached_data['routes']
            else:
                logger.debug(f"Cache expired for route: {origin} -> {destination}")

        return None

    def cache_api_response(self, api_name: str, request_params: Dict, response_data: Dict):
        """Cache raw API responses to avoid duplicate calls"""
        cache_key = self._generate_cache_key({
            'api': api_name,
            'params': request_params
        })

        cache_entry = {
            'timestamp': datetime.now().isoformat(),
            'api_name': api_name,
            'request_params': request_params,
            'response_data': response_data,
            'cache_key': cache_key
        }

        s3_key = f"{self.cache_prefixes['api_responses']}{api_name}_{cache_key}.json"
        save_json_to_data_lake(self.bucket_name, s3_key, cache_entry)

        # Short-term memory cache for API responses
        self._set_memory_cache(f"api_{cache_key}", cache_entry, self.config.api_responses_ttl_minutes)

        logger.debug(f"Cached API response: {api_name}")

    def get_cached_api_response(self, api_name: str, request_params: Dict) -> Optional[Dict]:
        """Get cached API response"""
        cache_key = self._generate_cache_key({
            'api': api_name,
            'params': request_params
        })

        # Check memory cache first
        memory_key = f"api_{cache_key}"
        cached = self._get_from_memory_cache(memory_key)
        if cached:
            return cached['response_data']

        # Check data lake
        s3_key = f"{self.cache_prefixes['api_responses']}{api_name}_{cache_key}.json"
        cached_data = read_json_from_data_lake(self.bucket_name, s3_key)

        if cached_data:
            if self._is_cache_valid(cached_data['timestamp'], self.config.api_responses_ttl_minutes):
                self._set_memory_cache(memory_key, cached_data, self.config.api_responses_ttl_minutes)
                logger.debug(f"API cache hit: {api_name}")
                return cached_data['response_data']

        return None

    def build_travel_time_matrix(self, stations_df: pd.DataFrame, schedules_df: pd.DataFrame):
        """Pre-calculate travel times between popular stations"""
        if stations_df.empty:
            return

        # Focus on La Défense area stations
        la_defense_stations = stations_df[
            stations_df['name'].str.contains('La Défense|Défense|CNIT|Grande Arche|Esplanade',
                                             case=False, na=False)
        ]

        travel_times = {}

        for _, origin_station in la_defense_stations.iterrows():
            origin_name = origin_station['name']

            for _, dest_station in la_defense_stations.iterrows():
                dest_name = dest_station['name']

                if origin_name != dest_name:
                    # Calculate estimated travel time based on coordinates and available transport
                    travel_time = self._estimate_travel_time(origin_station, dest_station, schedules_df)

                    route_key = f"{origin_name}|{dest_name}"
                    travel_times[route_key] = {
                        'origin': origin_name,
                        'destination': dest_name,
                        'estimated_time_minutes': travel_time,
                        'calculated_at': datetime.now().isoformat()
                    }

        # Save travel time matrix
        matrix_data = {
            'timestamp': datetime.now().isoformat(),
            'travel_times': travel_times,
            'station_count': len(la_defense_stations)
        }

        s3_key = f"{self.cache_prefixes['travel_times']}matrix_latest.json"
        save_json_to_data_lake(self.bucket_name, s3_key, matrix_data)

        logger.info(f"Built travel time matrix for {len(travel_times)} station pairs")

    def _estimate_travel_time(self, origin_station: pd.Series, dest_station: pd.Series,
                              schedules_df: pd.DataFrame) -> int:
        """Estimate travel time between two stations"""
        # Simple distance-based calculation
        try:
            lat1, lon1 = float(origin_station.get('lat', 0)), float(origin_station.get('lon', 0))
            lat2, lon2 = float(dest_station.get('lat', 0)), float(dest_station.get('lon', 0))

            # Calculate distance (simplified)
            distance = ((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2) ** 0.5 * 111000  # Rough meters

            # Estimate time based on transport type availability
            if not schedules_df.empty:
                # Check what transport types are available
                available_types = schedules_df['transport_type'].unique()

                if 'metro' in available_types:
                    # Metro: ~35 km/h average
                    return max(2, int(distance / 35000 * 60))
                elif any(t in available_types for t in ['rer', 'rers']):
                    # RER: ~40 km/h average
                    return max(3, int(distance / 40000 * 60))
                else:
                    # Bus/other: ~20 km/h average
                    return max(5, int(distance / 20000 * 60))

            # Default walking estimate if no transport data
            return max(10, int(distance / 5000 * 60))  # ~5 km/h walking

        except:
            return 15  # Default 15 minutes

    def track_popular_routes(self, origin: str, destination: str):
        """Track route popularity for better caching"""
        route_key = f"{origin}|{destination}"

        # Try to get existing popularity data
        s3_key = f"{self.cache_prefixes['popular_routes']}popularity.json"
        popularity_data = read_json_from_data_lake(self.bucket_name, s3_key)

        if not popularity_data:
            popularity_data = {
                'timestamp': datetime.now().isoformat(),
                'routes': {}
            }

        # Update route count
        if route_key not in popularity_data['routes']:
            popularity_data['routes'][route_key] = {
                'count': 0,
                'last_requested': datetime.now().isoformat(),
                'origin': origin,
                'destination': destination
            }

        popularity_data['routes'][route_key]['count'] += 1
        popularity_data['routes'][route_key]['last_requested'] = datetime.now().isoformat()
        popularity_data['timestamp'] = datetime.now().isoformat()

        # Save updated popularity data
        save_json_to_data_lake(self.bucket_name, s3_key, popularity_data)

        logger.debug(f"Tracked route popularity: {route_key}")

    def get_popular_routes(self, limit: int = 10) -> List[Dict]:
        """Get most popular routes for pre-caching"""
        s3_key = f"{self.cache_prefixes['popular_routes']}popularity.json"
        popularity_data = read_json_from_data_lake(self.bucket_name, s3_key)

        if not popularity_data:
            return []

        # Sort routes by popularity
        routes = list(popularity_data['routes'].values())
        routes.sort(key=lambda x: x['count'], reverse=True)

        return routes[:limit]

    def pre_cache_popular_routes(self, route_planner, stations_df: pd.DataFrame,
                                 schedules_df: pd.DataFrame):
        """Pre-calculate and cache popular routes"""
        popular_routes = self.get_popular_routes()

        default_preferences = {
            'time_pref': 1.0,
            'transfer_pref': 0.3,
            'comfort_pref': 0.5,
            'cost_pref': 0.2,
            'eco_pref': 0.2,
            'accessibility': False
        }

        transport_modes = ['Metro', 'RER', 'Bus']

        for route_info in popular_routes:
            origin = route_info['origin']
            destination = route_info['destination']

            # Check if already cached
            cached = self.get_cached_route(origin, destination, default_preferences, transport_modes)
            if cached:
                continue  # Already cached

            try:
                # Calculate and cache the route
                logger.info(f"Pre-caching popular route: {origin} -> {destination}")
                routes = route_planner.plan_routes(
                    origin=origin,
                    destination=destination,
                    preferences=default_preferences,
                    transport_modes=transport_modes,
                    stations_df=stations_df,
                    schedules_df=schedules_df
                )

                if routes:
                    # Convert to format expected by cache
                    route_dict = {}
                    for i, route in enumerate(routes):
                        route_dict[f"Route_{i + 1}"] = {
                            'total_time': route.total_duration_minutes,
                            'num_transfers': route.num_transfers,
                            'total_emissions': route.total_emissions_g,
                            'accessibility_score': route.accessibility_score
                        }

                    self.cache_route_result(origin, destination, default_preferences,
                                            transport_modes, route_dict)

            except Exception as e:
                logger.error(f"Error pre-caching route {origin}->{destination}: {e}")

    def cleanup_expired_cache(self):
        """Remove expired cache entries to save storage"""
        from utils.data_lake_utils import list_files_in_data_lake

        current_time = datetime.now()
        deleted_count = 0

        for cache_type, prefix in self.cache_prefixes.items():
            try:
                files = list_files_in_data_lake(self.bucket_name, prefix)

                for file_key in files:
                    try:
                        cached_data = read_json_from_data_lake(self.bucket_name, file_key)
                        if cached_data and 'timestamp' in cached_data:
                            cache_time = datetime.fromisoformat(cached_data['timestamp'])

                            # Get appropriate TTL
                            ttl_minutes = getattr(self.config, f"{cache_type}_ttl_minutes", 60)
                            expiry_time = cache_time + timedelta(minutes=ttl_minutes)

                            if current_time > expiry_time:
                                # Delete expired cache entry
                                self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
                                deleted_count += 1
                                logger.debug(f"Deleted expired cache: {file_key}")

                    except Exception as e:
                        logger.warning(f"Error checking cache file {file_key}: {e}")

            except Exception as e:
                logger.error(f"Error cleaning up cache type {cache_type}: {e}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} expired cache entries")

    def get_cache_stats(self) -> Dict:
        """Get cache usage statistics"""
        from utils.data_lake_utils import list_files_in_data_lake

        stats = {
            'cache_types': {},
            'total_cached_items': 0,
            'memory_cache_items': len(self.memory_cache),
            'generated_at': datetime.now().isoformat()
        }

        for cache_type, prefix in self.cache_prefixes.items():
            try:
                files = list_files_in_data_lake(self.bucket_name, prefix)
                valid_files = 0
                expired_files = 0

                ttl_minutes = getattr(self.config, f"{cache_type}_ttl_minutes", 60)

                for file_key in files:
                    try:
                        cached_data = read_json_from_data_lake(self.bucket_name, file_key)
                        if cached_data and 'timestamp' in cached_data:
                            if self._is_cache_valid(cached_data['timestamp'], ttl_minutes):
                                valid_files += 1
                            else:
                                expired_files += 1
                    except:
                        expired_files += 1

                stats['cache_types'][cache_type] = {
                    'total_files': len(files),
                    'valid_files': valid_files,
                    'expired_files': expired_files,
                    'ttl_minutes': ttl_minutes
                }

                stats['total_cached_items'] += valid_files

            except Exception as e:
                logger.error(f"Error getting stats for {cache_type}: {e}")
                stats['cache_types'][cache_type] = {'error': str(e)}

        return stats


# Enhanced Route Planner with Caching Integration
class CachedRoutePlanner:
    """Route planner with intelligent caching"""

    def __init__(self, route_planner, cache_config: CacheConfig = None):
        self.route_planner = route_planner
        self.cache = RouteCache(cache_config)

    def plan_routes_cached(self, origin: str, destination: str, preferences: Dict,
                           transport_modes: List[str], stations_df: pd.DataFrame,
                           schedules_df: pd.DataFrame, traffic_df: pd.DataFrame = None) -> Dict:
        """Plan routes with intelligent caching"""

        # Track popularity
        self.cache.track_popular_routes(origin, destination)

        # Try to get from cache first
        cached_routes = self.cache.get_cached_route(origin, destination, preferences, transport_modes)
        if cached_routes:
            logger.info(f"Using cached route: {origin} -> {destination}")
            return cached_routes

        # Cache miss - calculate new routes
        logger.info(f"Calculating new route: {origin} -> {destination}")

        try:
            routes = self.route_planner.plan_routes(
                origin=origin,
                destination=destination,
                preferences=preferences,
                transport_modes=transport_modes,
                stations_df=stations_df,
                schedules_df=schedules_df
            )

            if routes:
                # Convert to cacheable format
                route_dict = {}
                for i, route in enumerate(routes):
                    route_name = f"Route_{i + 1}"
                    route_dict[route_name] = {
                        'total_time': route.total_duration_minutes,
                        'num_transfers': route.num_transfers,
                        'total_emissions': route.total_emissions_g,
                        'accessibility_score': route.accessibility_score,
                        'route_details': [asdict(step) for step in route.steps]
                    }

                # Cache the result
                self.cache.cache_route_result(origin, destination, preferences,
                                              transport_modes, route_dict)

                return route_dict

            return {"No routes found": {"message": "No routes available"}}

        except Exception as e:
            logger.error(f"Error in cached route planning: {e}")
            return {"Error": {"message": f"Route planning failed: {str(e)}"}}

    def cached_api_call(self, api_name: str, api_function, request_params: Dict):
        """Make API call with caching"""
        # Check cache first
        cached_response = self.cache.get_cached_api_response(api_name, request_params)
        if cached_response:
            logger.debug(f"Using cached API response: {api_name}")
            return cached_response

        # Make actual API call
        try:
            response = api_function(**request_params)
            if response:
                # Cache the response
                self.cache.cache_api_response(api_name, request_params, response)
            return response
        except Exception as e:
            logger.error(f"API call failed for {api_name}: {e}")
            return None
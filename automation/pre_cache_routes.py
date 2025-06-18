import logging
from datetime import datetime
import sys
import os

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.route_cache import RouteCache, CachedRoutePlanner, CacheConfig
from utils.route_planner import RealRoutePlanner
from utils.data_lake_utils import read_parquet_from_data_lake
from configuration.config import DATA_LAKE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PreCacheService:
    """Service for pre-caching popular routes"""

    def __init__(self):
        self.cache_config = CacheConfig()
        self.base_planner = RealRoutePlanner()
        self.cached_planner = CachedRoutePlanner(self.base_planner, self.cache_config)

        # Common La Défense destinations
        self.la_defense_stations = [
            "La Défense Grande Arche",
            "Esplanade de La Défense",
            "La Défense",
            "CNIT",
            "Grande Arche"
        ]

        # Popular destinations from La Défense
        self.popular_destinations = [
            "Châtelet-Les Halles",
            "Charles de Gaulle-Étoile",
            "Nation",
            "Bastille",
            "République",
            "Gare du Nord",
            "Gare de Lyon",
            "Montparnasse",
            "Opéra",
            "Louvre-Rivoli",
            "Saint-Lazare",
            "Concorde",
            "Champs-Élysées",
            "Trocadéro",
            "Invalides"
        ]

        # Common transport mode combinations
        self.transport_combinations = [
            ["Metro", "RER"],
            ["Metro", "RER", "Bus"],
            ["RER"],
            ["Metro"],
            ["Bus"],
            ["Metro", "RER", "Transilien"]
        ]

        # Common preference profiles
        self.preference_profiles = [
            {
                'name': 'speed_focused',
                'preferences': {
                    'time_pref': 1.0,
                    'transfer_pref': 0.2,
                    'comfort_pref': 0.3,
                    'cost_pref': 0.1,
                    'eco_pref': 0.1,
                    'accessibility': False
                }
            },
            {
                'name': 'eco_friendly',
                'preferences': {
                    'time_pref': 0.6,
                    'transfer_pref': 0.3,
                    'comfort_pref': 0.4,
                    'cost_pref': 0.3,
                    'eco_pref': 1.0,
                    'accessibility': False
                }
            },
            {
                'name': 'accessible',
                'preferences': {
                    'time_pref': 0.7,
                    'transfer_pref': 0.8,
                    'comfort_pref': 0.9,
                    'cost_pref': 0.2,
                    'eco_pref': 0.3,
                    'accessibility': True
                }
            },
            {
                'name': 'budget_conscious',
                'preferences': {
                    'time_pref': 0.5,
                    'transfer_pref': 0.4,
                    'comfort_pref': 0.3,
                    'cost_pref': 1.0,
                    'eco_pref': 0.4,
                    'accessibility': False
                }
            }
        ]

    def load_data(self):
        """Load required data from data lake"""
        bucket_name = DATA_LAKE["bucket_name"]

        try:
            # Try to load combined data first
            stations_df = read_parquet_from_data_lake(bucket_name, 'refined/stations/combined_stations_latest.parquet')
            schedules_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/schedules_latest.parquet')

            if stations_df.empty:
                # Fallback to individual sources
                stations_df = read_parquet_from_data_lake(bucket_name,
                                                          'refined/stations/ratp_osm_combined_latest.parquet')

            if schedules_df.empty:
                # Try alternative schedule sources
                schedules_df = read_parquet_from_data_lake(bucket_name,
                                                           'refined/transport/ratp_schedules_latest.parquet')

            logger.info(f"Loaded {len(stations_df)} stations and {len(schedules_df)} schedule entries")
            return stations_df, schedules_df

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None, None

    def pre_cache_popular_combinations(self):
        """Pre-cache popular route combinations"""
        stations_df, schedules_df = self.load_data()

        if stations_df is None or schedules_df is None:
            logger.error("Could not load required data for pre-caching")
            return

        total_combinations = 0
        cached_combinations = 0

        logger.info("Starting pre-cache process...")

        # Pre-cache routes FROM La Défense to popular destinations
        for origin in self.la_defense_stations:
            for destination in self.popular_destinations:
                for transport_modes in self.transport_combinations:
                    for profile in self.preference_profiles:

                        total_combinations += 1

                        # Check if already cached
                        cached = self.cached_planner.cache.get_cached_route(
                            origin, destination, profile['preferences'], transport_modes
                        )

                        if cached:
                            logger.debug(f"Already cached: {origin} -> {destination} ({profile['name']})")
                            continue

                        try:
                            logger.info(f"Pre-caching: {origin} -> {destination} "
                                        f"({profile['name']}, {transport_modes})")

                            routes = self.cached_planner.plan_routes_cached(
                                origin=origin,
                                destination=destination,
                                preferences=profile['preferences'],
                                transport_modes=transport_modes,
                                stations_df=stations_df,
                                schedules_df=schedules_df
                            )

                            if routes and "Error" not in routes:
                                cached_combinations += 1
                                logger.debug(f"Successfully cached route")
                            else:
                                logger.warning(f"No routes found for {origin} -> {destination}")

                        except Exception as e:
                            logger.error(f"Error pre-caching route {origin} -> {destination}: {e}")

        # Pre-cache routes TO La Défense from popular origins
        for destination in self.la_defense_stations:
            for origin in self.popular_destinations[:5]:  # Limit to top 5 to avoid too many combinations
                for transport_modes in self.transport_combinations[:3]:  # Limit transport combinations
                    for profile in self.preference_profiles[:2]:  # Limit to 2 most common profiles

                        total_combinations += 1

                        # Check if already cached
                        cached = self.cached_planner.cache.get_cached_route(
                            origin, destination, profile['preferences'], transport_modes
                        )

                        if cached:
                            continue

                        try:
                            logger.info(f"Pre-caching: {origin} -> {destination} "
                                        f"({profile['name']}, {transport_modes})")

                            routes = self.cached_planner.plan_routes_cached(
                                origin=origin,
                                destination=destination,
                                preferences=profile['preferences'],
                                transport_modes=transport_modes,
                                stations_df=stations_df,
                                schedules_df=schedules_df
                            )

                            if routes and "Error" not in routes:
                                cached_combinations += 1

                        except Exception as e:
                            logger.error(f"Error pre-caching route {origin} -> {destination}: {e}")

        logger.info(f"Pre-caching completed: {cached_combinations}/{total_combinations} routes cached")

        # Generate cache report
        stats = self.cached_planner.cache.get_cache_stats()
        logger.info(f"Total cached items after pre-caching: {stats['total_cached_items']}")

    def warm_api_cache(self):
        """Warm up API response cache with common requests"""
        logger.info("Warming up API cache...")

        # Common API requests to pre-cache
        common_stations = self.la_defense_stations + self.popular_destinations[:10]

        for station in common_stations:
            try:
                # Cache station search results
                params = {'station': station.lower().replace(' ', '+')}

                # This would make actual API calls and cache them
                # Implementation depends on your specific API structure
                logger.debug(f"Warming API cache for station: {station}")

            except Exception as e:
                logger.warning(f"Could not warm API cache for {station}: {e}")


def main():
    """Main pre-caching function"""
    logger.info("=== STARTING PRE-CACHE SERVICE ===")
    start_time = datetime.now()

    service = PreCacheService()

    # Pre-cache popular route combinations
    service.pre_cache_popular_combinations()

    # Warm up API cache
    service.warm_api_cache()

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"=== PRE-CACHING COMPLETED in {duration:.1f} seconds ===")


if __name__ == "__main__":
    main()

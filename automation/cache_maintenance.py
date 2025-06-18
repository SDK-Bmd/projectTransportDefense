import schedule
import time
import logging
from datetime import datetime
import sys
import os

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.route_cache import RouteCache, CacheConfig
from utils.data_lake_utils import get_s3_client
from configuration.config import DATA_LAKE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cache_maintenance.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CacheMaintenanceService:
    """Automated cache maintenance service"""

    def __init__(self):
        self.cache_config = CacheConfig(
            routes_ttl_minutes=60,
            stations_ttl_minutes=1440,
            schedules_ttl_minutes=15,
            popular_routes_ttl_minutes=30,
            api_responses_ttl_minutes=10
        )
        self.route_cache = RouteCache(self.cache_config)

    def cleanup_expired_cache(self):
        """Clean up expired cache entries"""
        logger.info("Starting cache cleanup...")
        try:
            self.route_cache.cleanup_expired_cache()
            logger.info("Cache cleanup completed successfully")
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")

    def generate_cache_report(self):
        """Generate cache usage report"""
        logger.info("Generating cache report...")
        try:
            stats = self.route_cache.get_cache_stats()

            logger.info("=== CACHE USAGE REPORT ===")
            logger.info(f"Total cached items: {stats['total_cached_items']}")
            logger.info(f"Memory cache items: {stats['memory_cache_items']}")

            for cache_type, type_stats in stats['cache_types'].items():
                if 'error' not in type_stats:
                    logger.info(f"{cache_type.upper()}:")
                    logger.info(f"  - Valid files: {type_stats['valid_files']}")
                    logger.info(f"  - Expired files: {type_stats['expired_files']}")
                    logger.info(f"  - TTL: {type_stats['ttl_minutes']} minutes")
                else:
                    logger.warning(f"{cache_type.upper()}: {type_stats['error']}")

            return stats

        except Exception as e:
            logger.error(f"Failed to generate cache report: {e}")
            return None

    def optimize_cache_settings(self):
        """Analyze usage patterns and suggest optimizations"""
        logger.info("Analyzing cache usage patterns...")

        try:
            popular_routes = self.route_cache.get_popular_routes(20)

            if popular_routes:
                logger.info("TOP 10 MOST POPULAR ROUTES:")
                for i, route in enumerate(popular_routes[:10], 1):
                    logger.info(f"{i}. {route['origin']} -> {route['destination']} "
                                f"(requested {route['count']} times)")

                # Suggest optimizations
                high_usage_routes = [r for r in popular_routes if r['count'] > 10]
                if high_usage_routes:
                    logger.info(f"RECOMMENDATION: Consider increasing cache TTL for "
                                f"{len(high_usage_routes)} high-usage routes")
            else:
                logger.info("No popular routes data available yet")

        except Exception as e:
            logger.error(f"Cache optimization analysis failed: {e}")

    def build_travel_time_matrix(self):
        """Build/update travel time matrix for popular stations"""
        logger.info("Building travel time matrix...")
        try:
            # Load station and schedule data
            from utils.data_lake_utils import read_parquet_from_data_lake

            bucket_name = DATA_LAKE["bucket_name"]

            try:
                stations_df = read_parquet_from_data_lake(bucket_name,
                                                          'refined/stations/combined_stations_latest.parquet')
                schedules_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/schedules_latest.parquet')

                if not stations_df.empty and not schedules_df.empty:
                    self.route_cache.build_travel_time_matrix(stations_df, schedules_df)
                    logger.info("Travel time matrix updated successfully")
                else:
                    logger.warning("Insufficient data to build travel time matrix")

            except Exception as e:
                logger.warning(f"Could not load data for travel time matrix: {e}")

        except Exception as e:
            logger.error(f"Failed to build travel time matrix: {e}")

    def run_maintenance_cycle(self):
        """Run complete maintenance cycle"""
        logger.info("=== STARTING CACHE MAINTENANCE CYCLE ===")
        start_time = datetime.now()

        # Generate report before cleanup
        self.generate_cache_report()

        # Clean up expired entries
        self.cleanup_expired_cache()

        # Update travel time matrix
        self.build_travel_time_matrix()

        # Analyze usage patterns
        self.optimize_cache_settings()

        # Generate report after cleanup
        logger.info("=== POST-CLEANUP REPORT ===")
        self.generate_cache_report()

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"=== MAINTENANCE CYCLE COMPLETED in {duration:.1f} seconds ===")


def schedule_maintenance():
    """Schedule automated maintenance tasks"""
    maintenance_service = CacheMaintenanceService()

    # Schedule different maintenance tasks
    schedule.every(1).hours.do(maintenance_service.cleanup_expired_cache)
    schedule.every(6).hours.do(maintenance_service.generate_cache_report)
    schedule.every(12).hours.do(maintenance_service.build_travel_time_matrix)
    schedule.every().day.at("02:00").do(maintenance_service.run_maintenance_cycle)

    logger.info("Cache maintenance scheduler started")
    logger.info("Scheduled tasks:")
    logger.info("- Cache cleanup: Every 1 hour")
    logger.info("- Cache reports: Every 6 hours")
    logger.info("- Travel time matrix: Every 12 hours")
    logger.info("- Full maintenance: Daily at 2:00 AM")

    # Run initial maintenance
    maintenance_service.run_maintenance_cycle()

    # Keep scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    schedule_maintenance()
#!/usr/bin/env python3

"""
Block processing script for Balance Tracker.

This script replaces the SQL-based main() procedure to avoid long-running
queries that would violate statement timeout settings.
"""

import sys
import time
import argparse
import configparser
import psycopg2
from psycopg2 import sql
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BlockProcessor:
    def __init__(self, connection_string, schema='btracker_app'):
        self.connection_string = connection_string
        self.schema = schema
        self.connection = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.connection.autocommit = False
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a query and optionally fetch results."""
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            if fetch:
                result = cursor.fetchone()
                cursor.close()
                return result
            cursor.close()
        except Exception as e:
            if cursor:
                cursor.close()
            raise e
    
    def allow_processing(self):
        """Enable processing flag."""
        query = sql.SQL("SELECT {schema}.allowProcessing()").format(
            schema=sql.Identifier(self.schema)
        )
        self.execute_query(query)
        self.connection.commit()
        logger.info("Processing enabled")
    
    def continue_processing(self):
        """Check if processing should continue."""
        query = sql.SQL("SELECT {schema}.continueProcessing()").format(
            schema=sql.Identifier(self.schema)
        )
        result = self.execute_query(query, fetch=True)
        return result[0] if result else False
    
    def get_current_block_num(self, context_name):
        """Get the current block number for the context."""
        query = "SELECT hive.app_get_current_block_num(%s)"
        result = self.execute_query(query, (context_name,), fetch=True)
        return result[0] if result else 0
    
    def get_next_iteration(self, context_name, max_block_limit):
        """
        Call hive.app_next_iteration and return the blocks range.
        Returns tuple (first_block, last_block) or None if no blocks available.
        """
        query = """
            DO $$
            DECLARE
                _blocks_range hive.blocks_range;
            BEGIN
                CALL hive.app_next_iteration(
                    %s::hive.context_name,
                    _blocks_range,
                    _override_max_batch => NULL,
                    _limit => %s
                );
                
                -- Store result in a temporary table for retrieval
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_blocks_range (
                    first_block INT,
                    last_block INT
                );
                DELETE FROM temp_blocks_range;
                
                IF _blocks_range IS NOT NULL THEN
                    INSERT INTO temp_blocks_range VALUES (_blocks_range.first_block, _blocks_range.last_block);
                END IF;
            END $$;
        """
        
        # Execute the procedure
        self.execute_query(query, (context_name, max_block_limit))
        
        # Fetch the result
        fetch_query = "SELECT first_block, last_block FROM temp_blocks_range"
        result = self.execute_query(fetch_query, fetch=True)
        
        if result and result[0] is not None:
            return {'first_block': result[0], 'last_block': result[1]}
        return None
    
    def process_blocks(self, context_name, blocks_range):
        """Process a range of blocks."""
        query = sql.SQL("""
            SELECT {schema}.btracker_process_blocks(
                %s::hive.context_name,
                (%s, %s)::hive.blocks_range
            )
        """).format(schema=sql.Identifier(self.schema))
        
        self.execute_query(
            query,
            (context_name, blocks_range['first_block'], blocks_range['last_block'])
        )
    
    def run(self, context_name, max_block_limit=None):
        """Main processing loop."""
        self.connect()
        
        try:
            self.allow_processing()
            
            current_block = self.get_current_block_num(context_name)
            logger.info(f"Last block processed by application: {current_block}")
            
            if max_block_limit is not None:
                logger.info(f"Max block limit is specified as: {max_block_limit}")
            
            logger.info("Entering application main loop...")
            
            while True:
                # Check if we should continue processing
                if not self.continue_processing():
                    self.connection.rollback()
                    current_block = self.get_current_block_num(context_name)
                    logger.info(f"Exiting application main loop at processed block: {current_block}")
                    break
                
                # Get next iteration
                # Note: app_next_iteration commits internally, so no commit needed here
                blocks_range = self.get_next_iteration(context_name, max_block_limit)
                
                if blocks_range is None:
                    logger.info("Waiting for next block...")
                    # Small sleep to avoid tight loop
                    time.sleep(0.1)
                    continue
                
                # Process the blocks
                logger.info(f"Processing blocks {blocks_range['first_block']} to {blocks_range['last_block']}")
                self.process_blocks(context_name, blocks_range)
                
                # Note: No explicit commit here. The next call to app_next_iteration will commit
                # the transaction, maintaining coherence between HAF's state and the app's state.
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
            if self.connection:
                self.connection.rollback()
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            self.disconnect()


def main():
    parser = argparse.ArgumentParser(
        description='Process blocks for Balance Tracker HAF application'
    )
    parser.add_argument(
        '--context',
        type=str,
        default='btracker_app',
        help='HAF context name (default: btracker_app)'
    )
    parser.add_argument(
        '--schema',
        type=str,
        default='btracker_app',
        help='Database schema name (default: btracker_app)'
    )
    parser.add_argument(
        '--stop-at-block',
        type=int,
        default=None,
        help='Maximum block number to process (default: None, process indefinitely)'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='PostgreSQL host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--user',
        type=str,
        default='btracker_owner',
        help='PostgreSQL user (default: btracker_owner)'
    )
    parser.add_argument(
        '--database',
        type=str,
        default='haf_block_log',
        help='PostgreSQL database name (default: haf_block_log)'
    )
    parser.add_argument(
        '--url',
        type=str,
        default=None,
        help='Full PostgreSQL connection URL (overrides other connection options)'
    )
    
    args = parser.parse_args()
    
    # Build connection string
    if args.url:
        connection_string = args.url
    else:
        connection_string = (
            f"postgresql://{args.user}@{args.host}:{args.port}/{args.database}"
            f"?application_name=btracker_block_processing"
        )
    
    # Create processor and run
    processor = BlockProcessor(connection_string, schema=args.schema)
    
    try:
        processor.run(args.context, args.stop_at_block)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

import os
import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
try:
    import avro.schema
    import avro.io
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import schedule
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from typing import Dict, List, Optional, Any
import configparser
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Configuration class for database connections"""
    def __init__(self, host: str, port: int, database: str, username: str, password: str, driver: str = 'postgresql'):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        if self.driver == 'postgresql':
            return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.driver == 'mysql':
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.driver == 'sqlite':
            return f"sqlite:///{self.database}"
        else:
            raise ValueError(f"Unsupported database driver: {self.driver}")

class DataExporter:
    """Class to handle data export to various formats"""
    
    def __init__(self, output_dir: str = "exports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """Export DataFrame to CSV"""
        filepath = self.output_dir / f"{filename}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} rows to CSV: {filepath}")
        return str(filepath)
    
    def to_parquet(self, df: pd.DataFrame, filename: str) -> str:
        """Export DataFrame to Parquet"""
        filepath = self.output_dir / f"{filename}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Exported {len(df)} rows to Parquet: {filepath}")
        return str(filepath)
    
    def to_avro(self, df: pd.DataFrame, filename: str, schema_dict: Dict = None) -> str:
        """Export DataFrame to Avro format"""
        if not AVRO_AVAILABLE:
            raise ImportError("avro-python3 not installed. Run: pip install avro-python3")
        
        filepath = self.output_dir / f"{filename}.avro"
        
        # Convert DataFrame to records
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (int, float, str, bool)):
                    record[col] = value
                else:
                    record[col] = str(value)
            records.append(record)
        
        # Generate Avro schema if not provided
        if schema_dict is None:
            schema_dict = self._generate_avro_schema(df, filename)
        
        # Write using fastavro (simpler alternative)
        try:
            import fastavro
            
            with open(filepath, 'wb') as f:
                fastavro.writer(f, schema_dict, records)
            
            logger.info(f"Exported {len(df)} rows to Avro: {filepath}")
            return str(filepath)
            
        except ImportError:
            # Fallback to avro-python3
            import avro.schema
            import avro.io
            
            schema = avro.schema.parse(json.dumps(schema_dict))
            
            with open(filepath, 'wb') as f:
                writer = avro.io.DatumWriter(schema)
                encoder = avro.io.BinaryEncoder(f)
                
                for record in records:
                    writer.write(record, encoder)
            
            logger.info(f"Exported {len(df)} rows to Avro: {filepath}")
            return str(filepath)
    
    def _generate_avro_schema(self, df: pd.DataFrame, table_name: str) -> Dict:
        """Generate Avro schema from DataFrame"""
        fields = []
        
        for col in df.columns:
            dtype = df[col].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                avro_type = ["null", "long"]
            elif pd.api.types.is_float_dtype(dtype):
                avro_type = ["null", "double"]
            elif pd.api.types.is_bool_dtype(dtype):
                avro_type = ["null", "boolean"]
            else:
                avro_type = ["null", "string"]
            
            fields.append({
                "name": col,
                "type": avro_type,
                "default": None
            })
        
        schema = {
            "type": "record",
            "name": table_name,
            "fields": fields
        }
        
        return schema

class DataPipeline:
    """Main data pipeline class"""
    
    def __init__(self, config_file: str = "pipeline_config.ini"):
        self.config = self._load_config(config_file)
        self.exporter = DataExporter()
        self.source_engines = {}
        self.dest_engines = {}
        
    def _load_config(self, config_file: str) -> configparser.ConfigParser:
        """Load configuration from file"""
        config = configparser.ConfigParser()
        
        # Create default config if file doesn't exist
        if not os.path.exists(config_file):
            self._create_default_config(config_file)
        
        config.read(config_file)
        return config
    
    def _create_default_config(self, config_file: str):
        """Create default configuration file"""
        config = configparser.ConfigParser()
        
        config['SOURCE_DB'] = {
            'host': 'localhost',
            'port': '5432',
            'database': 'source_db',
            'username': 'user',
            'password': 'password',
            'driver': 'postgresql'
        }
        
        config['DEST_DB'] = {
            'host': 'localhost',
            'port': '5432',
            'database': 'dest_db',
            'username': 'user',
            'password': 'password',
            'driver': 'postgresql'
        }
        
        config['EXPORT_SETTINGS'] = {
            'output_directory': 'exports',
            'export_formats': 'csv,parquet,avro',
            'batch_size': '10000'
        }
        
        config['SCHEDULE'] = {
            'enabled': 'true',
            'frequency': 'daily',
            'time': '02:00'
        }
        
        with open(config_file, 'w') as f:
            config.write(f)
        
        logger.info(f"Created default configuration file: {config_file}")
    
    def add_database_connection(self, name: str, db_config: DatabaseConfig):
        """Add database connection"""
        try:
            connection_string = db_config.get_connection_string()
            engine = create_engine(connection_string, echo=False)
            
            # Test the connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            if name.startswith('source'):
                self.source_engines[name] = engine
            else:
                self.dest_engines[name] = engine
            logger.info(f"Added database connection: {name}")
        except Exception as e:
            logger.error(f"Failed to add database connection {name}: {e}")
            raise
    
    def export_table_to_files(self, table_name: str, source_db: str = 'source', 
                            formats: List[str] = ['csv', 'parquet', 'avro'],
                            query: str = None, columns: List[str] = None) -> Dict[str, str]:
        """Export table data to multiple file formats"""
        
        if source_db not in self.source_engines:
            raise ValueError(f"Source database '{source_db}' not configured")
        
        engine = self.source_engines[source_db]
        
        # Build query
        if query:
            sql_query = query
        else:
            if columns:
                column_str = ', '.join(columns)
                sql_query = f"SELECT {column_str} FROM {table_name}"
            else:
                sql_query = f"SELECT * FROM {table_name}"
        
        # Execute query and load data
        try:
            # Try with pandas first
            df = pd.read_sql(sql_query, engine)
            logger.info(f"Loaded {len(df)} rows from {table_name}")
        except Exception as e:
            logger.warning(f"Pandas read_sql failed: {e}")
            try:
                # Fallback: use raw SQLAlchemy
                with engine.connect() as conn:
                    result = conn.execute(text(sql_query))
                    columns = result.keys()
                    data = result.fetchall()
                    df = pd.DataFrame(data, columns=columns)
                logger.info(f"Loaded {len(df)} rows from {table_name} (fallback method)")
            except Exception as e2:
                logger.error(f"Error loading data from {table_name}: {e2}")
                raise
        
        # Export to requested formats
        exported_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}"
        
        for fmt in formats:
            try:
                if fmt.lower() == 'csv':
                    filepath = self.exporter.to_csv(df, filename)
                    exported_files[fmt] = filepath
                elif fmt.lower() == 'parquet':
                    filepath = self.exporter.to_parquet(df, filename)
                    exported_files[fmt] = filepath
                elif fmt.lower() == 'avro':
                    filepath = self.exporter.to_avro(df, filename)
                    exported_files[fmt] = filepath
                else:
                    logger.warning(f"Unsupported format: {fmt}")
                    continue
                    
            except Exception as e:
                logger.error(f"Error exporting to {fmt}: {e}")
                # Continue with other formats even if one fails
                continue
        
        return exported_files
    
    def copy_table_to_database(self, table_name: str, source_db: str = 'source', 
                              dest_db: str = 'dest', columns: List[str] = None,
                              if_exists: str = 'replace') -> bool:
        """Copy table from source to destination database"""
        
        if source_db not in self.source_engines:
            raise ValueError(f"Source database '{source_db}' not configured")
        if dest_db not in self.dest_engines:
            raise ValueError(f"Destination database '{dest_db}' not configured")
        
        source_engine = self.source_engines[source_db]
        dest_engine = self.dest_engines[dest_db]
        
        try:
            # Build query
            if columns:
                column_str = ', '.join(columns)
                query = f"SELECT {column_str} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
            
            # Load data from source
            try:
                df = pd.read_sql(query, source_engine)
            except Exception as e:
                logger.warning(f"Pandas read_sql failed: {e}")
                # Fallback: use raw SQLAlchemy
                with source_engine.connect() as conn:
                    result = conn.execute(text(query))
                    columns = result.keys()
                    data = result.fetchall()
                    df = pd.DataFrame(data, columns=columns)
            
            logger.info(f"Loaded {len(df)} rows from {source_db}.{table_name}")
            
            # Write to destination
            try:
                df.to_sql(table_name, dest_engine, if_exists=if_exists, index=False)
            except Exception as e:
                logger.warning(f"Pandas to_sql failed: {e}")
                # Fallback: use raw SQLAlchemy
                with dest_engine.connect() as conn:
                    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            logger.info(f"Copied {len(df)} rows to {dest_db}.{table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error copying table {table_name}: {e}")
            return False
    
    def copy_all_tables(self, source_db: str = 'source', dest_db: str = 'dest',
                       exclude_tables: List[str] = None) -> Dict[str, bool]:
        """Copy all tables from source to destination database"""
        
        if source_db not in self.source_engines:
            raise ValueError(f"Source database '{source_db}' not configured")
        if dest_db not in self.dest_engines:
            raise ValueError(f"Destination database '{dest_db}' not configured")
        
        source_engine = self.source_engines[source_db]
        exclude_tables = exclude_tables or []
        
        # Get all table names
        inspector = inspect(source_engine)
        table_names = inspector.get_table_names()
        
        results = {}
        
        for table_name in table_names:
            if table_name in exclude_tables:
                logger.info(f"Skipping excluded table: {table_name}")
                continue
            
            try:
                success = self.copy_table_to_database(table_name, source_db, dest_db)
                results[table_name] = success
            except Exception as e:
                logger.error(f"Failed to copy table {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    def export_all_tables(self, source_db: str = 'source', 
                         formats: List[str] = ['csv', 'parquet', 'avro'],
                         exclude_tables: List[str] = None) -> Dict[str, Dict[str, str]]:
        """Export all tables to file formats"""
        
        if source_db not in self.source_engines:
            raise ValueError(f"Source database '{source_db}' not configured")
        
        source_engine = self.source_engines[source_db]
        exclude_tables = exclude_tables or []
        
        # Get all table names
        inspector = inspect(source_engine)
        table_names = inspector.get_table_names()
        
        results = {}
        
        for table_name in table_names:
            if table_name in exclude_tables:
                logger.info(f"Skipping excluded table: {table_name}")
                continue
            
            try:
                exported_files = self.export_table_to_files(
                    table_name, source_db, formats
                )
                results[table_name] = exported_files
            except Exception as e:
                logger.error(f"Failed to export table {table_name}: {e}")
                results[table_name] = {}
        
        return results

class EventHandler(FileSystemEventHandler):
    """File system event handler for trigger-based pipeline execution"""
    
    def __init__(self, pipeline: DataPipeline, trigger_config: Dict):
        self.pipeline = pipeline
        self.trigger_config = trigger_config
    
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"File created: {event.src_path}")
            self._execute_pipeline_action(event.src_path)
    
    def on_modified(self, event):
        if not event.is_directory:
            logger.info(f"File modified: {event.src_path}")
            self._execute_pipeline_action(event.src_path)
    
    def _execute_pipeline_action(self, filepath: str):
        """Execute pipeline action based on file event"""
        action = self.trigger_config.get('action', 'export_all')
        
        if action == 'export_all':
            self.pipeline.export_all_tables()
        elif action == 'copy_all':
            self.pipeline.copy_all_tables()
        
        logger.info(f"Pipeline action '{action}' completed for file: {filepath}")

class PipelineScheduler:
    """Scheduler for automated pipeline execution"""
    
    def __init__(self, pipeline: DataPipeline):
        self.pipeline = pipeline
        self.running = False
        self.observer = None
    
    def setup_schedule_triggers(self, frequency: str = 'daily', time_str: str = '02:00'):
        """Setup schedule-based triggers"""
        
        if frequency == 'daily':
            schedule.every().day.at(time_str).do(self._run_scheduled_pipeline)
        elif frequency == 'hourly':
            schedule.every().hour.do(self._run_scheduled_pipeline)
        elif frequency == 'weekly':
            schedule.every().week.do(self._run_scheduled_pipeline)
        
        logger.info(f"Scheduled pipeline to run {frequency} at {time_str}")
    
    def setup_event_triggers(self, watch_directory: str, trigger_config: Dict):
        """Setup event-based triggers"""
        
        event_handler = EventHandler(self.pipeline, trigger_config)
        self.observer = Observer()
        self.observer.schedule(event_handler, watch_directory, recursive=True)
        
        logger.info(f"Setup event triggers for directory: {watch_directory}")
    
    def _run_scheduled_pipeline(self):
        """Execute scheduled pipeline tasks"""
        logger.info("Running scheduled pipeline execution")
        
        try:
            # Export all tables
            export_results = self.pipeline.export_all_tables()
            logger.info(f"Exported {len(export_results)} tables")
            
            # Copy all tables (if destination is configured)
            if self.pipeline.dest_engines:
                copy_results = self.pipeline.copy_all_tables()
                logger.info(f"Copied {len(copy_results)} tables")
            
        except Exception as e:
            logger.error(f"Error in scheduled pipeline execution: {e}")
    
    def start(self):
        """Start the scheduler"""
        self.running = True
        
        # Start event observer if configured
        if self.observer:
            self.observer.start()
            logger.info("Event observer started")
        
        # Start schedule loop
        def schedule_loop():
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        schedule_thread = threading.Thread(target=schedule_loop)
        schedule_thread.daemon = True
        schedule_thread.start()
        
        logger.info("Pipeline scheduler started")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        logger.info("Pipeline scheduler stopped")

def create_demo_database():
    """Create demo SQLite databases with sample data for testing"""
    import sqlite3
    
    # Create source database
    source_conn = sqlite3.connect('source_demo.db')
    source_cursor = source_conn.cursor()
    
    # Create sample tables
    source_cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            age INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    source_cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            category TEXT,
            in_stock BOOLEAN DEFAULT TRUE
        )
    ''')
    
    source_cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
    ''')
    
    # Insert sample data
    users_data = [
        (1, 'John Doe', 'john@example.com', 30),
        (2, 'Jane Smith', 'jane@example.com', 25),
        (3, 'Bob Johnson', 'bob@example.com', 35),
        (4, 'Alice Brown', 'alice@example.com', 28),
        (5, 'Charlie Wilson', 'charlie@example.com', 42)
    ]
    
    products_data = [
        (1, 'Laptop', 999.99, 'Electronics', True),
        (2, 'Mouse', 29.99, 'Electronics', True),
        (3, 'Keyboard', 79.99, 'Electronics', True),
        (4, 'Monitor', 299.99, 'Electronics', False),
        (5, 'Desk Chair', 199.99, 'Furniture', True)
    ]
    
    orders_data = [
        (1, 1, 1, 1),
        (2, 1, 2, 2),
        (3, 2, 3, 1),
        (4, 3, 1, 1),
        (5, 4, 5, 1),
        (6, 5, 2, 3)
    ]
    
    source_cursor.executemany('INSERT OR REPLACE INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', users_data)
    source_cursor.executemany('INSERT OR REPLACE INTO products (id, name, price, category, in_stock) VALUES (?, ?, ?, ?, ?)', products_data)
    source_cursor.executemany('INSERT OR REPLACE INTO orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)', orders_data)
    
    source_conn.commit()
    source_conn.close()
    
    # Create destination database (empty)
    dest_conn = sqlite3.connect('dest_demo.db')
    dest_conn.close()
    
    logger.info("Demo databases created successfully!")
    logger.info("Source database: source_demo.db")
    logger.info("Destination database: dest_demo.db")

def main():
    """Main function to demonstrate pipeline usage"""
    
    # Create demo databases
    create_demo_database()
    
    # Initialize pipeline
    pipeline = DataPipeline()
    
    # Configure source database (SQLite demo)
    source_config = DatabaseConfig(
        host='localhost',
        port=5432,
        database='source_demo.db',  # SQLite file
        username='user',
        password='password',
        driver='sqlite'
    )
    pipeline.add_database_connection('source', source_config)
    
    # Configure destination database (SQLite demo)
    dest_config = DatabaseConfig(
        host='localhost',
        port=5432,
        database='dest_demo.db',  # SQLite file
        username='user',
        password='password',
        driver='sqlite'
    )
    pipeline.add_database_connection('dest', dest_config)
    
    # Example 1: Export specific table to all formats
    print("=== Example 1: Export table to files ===")
    try:
        # First try with all formats
        exported_files = pipeline.export_table_to_files(
            table_name='users',
            formats=['csv', 'parquet', 'avro']
        )
        print(f"Exported files: {exported_files}")
        
        # If avro fails, try without it
        if not exported_files or 'avro' not in exported_files:
            print("Trying without Avro format...")
            exported_files = pipeline.export_table_to_files(
                table_name='users',
                formats=['csv', 'parquet']
            )
            print(f"Exported files (without Avro): {exported_files}")
            
    except Exception as e:
        print(f"Export failed: {e}")
        # Try with just CSV and Parquet
        try:
            exported_files = pipeline.export_table_to_files(
                table_name='users',
                formats=['csv', 'parquet']
            )
            print(f"Exported files (CSV and Parquet only): {exported_files}")
        except Exception as e2:
            print(f"Even basic export failed: {e2}")
    
    # Example 2: Copy specific table with selected columns
    print("\n=== Example 2: Copy specific table with selected columns ===")
    try:
        success = pipeline.copy_table_to_database(
            table_name='users',
            columns=['id', 'name', 'email'],
            if_exists='replace'
        )
        print(f"Copy successful: {success}")
    except Exception as e:
        print(f"Copy failed: {e}")
    
    # Example 3: Copy all tables
    print("\n=== Example 3: Copy all tables ===")
    try:
        copy_results = pipeline.copy_all_tables(
            exclude_tables=['temp_table', 'logs']
        )
        print(f"Copy results: {copy_results}")
    except Exception as e:
        print(f"Copy all failed: {e}")
    
    # Example 4: Export all tables
    print("\n=== Example 4: Export all tables ===")
    try:
        export_results = pipeline.export_all_tables(
            formats=['csv', 'parquet'],  # Skip avro for now
            exclude_tables=['temp_table']
        )
        print(f"Export results: {export_results}")
    except Exception as e:
        print(f"Export all failed: {e}")
    
    # Example 5: Setup automated pipeline
    print("\n=== Example 5: Setup automated pipeline ===")
    try:
        scheduler = PipelineScheduler(pipeline)
        
        # Setup schedule triggers
        scheduler.setup_schedule_triggers(frequency='daily', time_str='02:00')
        
        # Setup event triggers (create a test directory)
        import os
        watch_dir = 'watch_directory'
        os.makedirs(watch_dir, exist_ok=True)
        
        scheduler.setup_event_triggers(
            watch_directory=watch_dir,
            trigger_config={'action': 'export_all'}
        )
        
        print("Scheduler configured successfully!")
        print("To start the scheduler, uncomment the scheduler.start() lines in the code")
        
    except Exception as e:
        print(f"Scheduler setup failed: {e}")
    
    # Display created files
    print("\n=== Created Files ===")
    import os
    current_dir = os.getcwd()
    files = [f for f in os.listdir(current_dir) if f.endswith(('.csv', '.parquet', '.avro', '.db'))]
    for file in files:
        print(f"- {file}")
    
    # Start scheduler (uncomment to run)
    # scheduler.start()
    
    # Keep the program running (uncomment to run)
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     scheduler.stop()
    #     print("Pipeline stopped")

if __name__ == "__main__":
    main()
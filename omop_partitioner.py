import os
import logging
import random
import string
import socket
from typing import List, Dict, Set, Tuple
import networkx as nx
from sqlalchemy import create_engine, text, MetaData, inspect
import docker
from dotenv import load_dotenv
import time
from distribution_strategies import (
    DistributionStrategy,
    UniformDistributionStrategy,
    HashDistributionStrategy,
    RoundRobinDistributionStrategy
)
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortManager:
    def __init__(self, start_port: int = 5432):
        self.start_port = start_port
        self.used_ports = set()
        self.port_range = range(start_port, start_port + 100)  # Allow up to 100 ports
    
    def find_available_port(self) -> int:
        """Find an available port starting from start_port"""
        # First try the default PostgreSQL port
        if self._is_port_available(5432) and 5432 not in self.used_ports:
            self.used_ports.add(5432)
            return 5432
            
        # Then try ports in our range
        for port in self.port_range:
            if port not in self.used_ports and self._is_port_available(port):
                self.used_ports.add(port)
                return port
                
        # If no ports are available in our range, try any available port
        port = 5432
        while port in self.used_ports or not self._is_port_available(port):
            port += 1
        self.used_ports.add(port)
        return port
    
    def _is_port_available(self, port: int) -> bool:
        """Check if a port is available by attempting to bind to it"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return True
            except socket.error:
                return False
    
    def release_port(self, port: int):
        """Release a port back to the pool"""
        if port in self.used_ports:
            self.used_ports.remove(port)
    
    def get_used_ports(self) -> Set[int]:
        """Get the set of currently used ports"""
        return self.used_ports.copy()

class CredentialManager:
    def __init__(self):
        self.credentials = {}
    
    def generate_credentials(self, prefix: str) -> Dict[str, str]:
        """Generate random credentials for a database"""
        username = f"{prefix}_{self._generate_random_string(8)}"
        password = self._generate_random_string(16)
        self.credentials[username] = password
        return {
            "username": username,
            "password": password
        }
    
    def _generate_random_string(self, length: int) -> str:
        """Generate a random string of specified length"""
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

class SchemaValidator:
    def __init__(self, source_engine, partition_engines: List[Tuple[int, object]]):
        self.source_engine = source_engine
        self.partition_engines = partition_engines
        self.source_metadata = MetaData()
        self.source_metadata.reflect(bind=source_engine)
    
    def validate_schema_compliance(self) -> bool:
        """Validate that each partition's schema matches the source"""
        validation_passed = True
        
        for partition_index, engine in self.partition_engines:
            partition_metadata = MetaData()
            partition_metadata.reflect(bind=engine)
            
            # Compare tables
            source_tables = set(self.source_metadata.tables.keys())
            partition_tables = set(partition_metadata.tables.keys())
            
            if source_tables != partition_tables:
                logger.error(f"Partition {partition_index}: Table mismatch")
                logger.error(f"Missing tables: {source_tables - partition_tables}")
                logger.error(f"Extra tables: {partition_tables - source_tables}")
                validation_passed = False
                continue
            
            # Compare columns and constraints for each table
            for table_name in source_tables:
                source_table = self.source_metadata.tables[table_name]
                partition_table = partition_metadata.tables[table_name]
                
                # Compare columns
                source_columns = {c.name: c for c in source_table.columns}
                partition_columns = {c.name: c for c in partition_table.columns}
                
                if source_columns.keys() != partition_columns.keys():
                    logger.error(f"Partition {partition_index}, Table {table_name}: Column mismatch")
                    validation_passed = False
                    continue
                
                # Compare constraints
                source_constraints = {c.name: c for c in source_table.constraints}
                partition_constraints = {c.name: c for c in partition_table.constraints}
                
                if source_constraints.keys() != partition_constraints.keys():
                    logger.error(f"Partition {partition_index}, Table {table_name}: Constraint mismatch")
                    validation_passed = False
        
        return validation_passed
    
    def validate_data_integrity(self) -> bool:
        """Validate that all partitions together match the source data"""
        validation_passed = True
        
        # Get source counts and sample data
        source_data = {}
        with self.source_engine.connect() as conn:
            for table_name in self.source_metadata.tables.keys():
                # Get count
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                source_data[table_name] = {
                    'count': count_result.scalar(),
                    'sample': self._get_sample_data(conn, table_name)
                }
        
        # Get partition counts and sample data
        partition_data = {table_name: {'count': 0, 'samples': []} for table_name in source_data.keys()}
        
        for partition_index, engine in self.partition_engines:
            with engine.connect() as conn:
                for table_name in source_data.keys():
                    # Get count
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    partition_data[table_name]['count'] += count_result.scalar()
                    
                    # Get sample
                    partition_data[table_name]['samples'].extend(
                        self._get_sample_data(conn, table_name)
                    )
        
        # Validate counts and data
        for table_name, source_info in source_data.items():
            partition_info = partition_data[table_name]
            
            # Check counts
            if source_info['count'] != partition_info['count']:
                logger.error(f"Table {table_name}: Count mismatch")
                logger.error(f"Source: {source_info['count']}, Partitions: {partition_info['count']}")
                validation_passed = False
            
            # Check data integrity using sample comparison
            if not self._compare_samples(source_info['sample'], partition_info['samples']):
                logger.error(f"Table {table_name}: Data integrity check failed")
                validation_passed = False
        
        return validation_passed
    
    def _get_sample_data(self, conn, table_name: str, sample_size: int = 100) -> List[tuple]:
        """Get a sample of data from a table"""
        try:
            result = conn.execute(text(
                f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {sample_size}"
            ))
            return result.fetchall()
        except Exception as e:
            logger.warning(f"Could not get sample data for {table_name}: {str(e)}")
            return []
    
    def _compare_samples(self, source_samples: List[tuple], partition_samples: List[tuple]) -> bool:
        """Compare sample data between source and partitions"""
        if not source_samples or not partition_samples:
            return True
        
        # Convert to sets of tuples for comparison
        source_set = {tuple(row) for row in source_samples}
        partition_set = {tuple(row) for row in partition_samples}
        
        # Check if all source samples exist in partitions
        return source_set.issubset(partition_set)

class OMOPPartitioner:
    def __init__(self, source_db_url: str, num_partitions: int, distribution_strategy: str = 'uniform'):
        """
        Initialize the OMOP partitioner
        
        Args:
            source_db_url: Connection string for the source database
            num_partitions: Number of partitions to create
            distribution_strategy: Strategy to use for data distribution ('uniform', 'hash', or 'round_robin')
        """
        self.source_db_url = source_db_url
        self.num_partitions = num_partitions
        self.source_engine = create_engine(source_db_url)
        self.docker_client = docker.from_env()
        self.person_table = 'omopcdm.person'  # Main table for partitioning with schema
        self.partition_containers = []
        self.port_manager = PortManager()
        self.credential_manager = CredentialManager()
        self.partition_engines = []
        self.postgres_version = "16"
        self.distribution_strategy = self._get_distribution_strategy(distribution_strategy)
        
        # Parse source database URL to get connection details
        parsed_url = urlparse(source_db_url)
        self.db_host = parsed_url.hostname or 'localhost'
        self.db_port = parsed_url.port or 5432
        self.db_name = parsed_url.path.lstrip('/')
        self.db_user = parsed_url.username
        self.db_password = parsed_url.password
        
        # Add the source database port to used ports
        self.port_manager.used_ports.add(self.db_port)
    
    def _get_distribution_strategy(self, strategy_name: str) -> DistributionStrategy:
        """Get the appropriate distribution strategy"""
        strategies = {
            'uniform': UniformDistributionStrategy,
            'hash': HashDistributionStrategy,
            'round_robin': RoundRobinDistributionStrategy
        }
        
        if strategy_name not in strategies:
            logger.warning(f"Unknown distribution strategy '{strategy_name}', using uniform distribution")
            strategy_name = 'uniform'
        
        return strategies[strategy_name]
    
    def analyze_schema(self) -> nx.DiGraph:
        """
        Analyze the database schema to build a dependency graph
        Returns a directed graph representing table relationships
        """
        graph = nx.DiGraph()
        
        # Query to get foreign key relationships, including schema
        query = """
        SELECT
            tc.table_schema,
            tc.table_name, 
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY';
        """
        
        with self.source_engine.connect() as conn:
            result = conn.execute(text(query))
            for row in result:
                from_table = f"{row[3]}.{row[4]}"
                to_table = f"{row[0]}.{row[1]}"
                graph.add_edge(from_table, to_table)
        
        return graph
    
    def get_related_tables(self, graph: nx.DiGraph) -> Set[str]:
        """
        Get all tables that are related to the person table
        """
        related_tables = set()
        for node in nx.descendants(graph, self.person_table):
            related_tables.add(node)
        related_tables.add(self.person_table)
        return related_tables
    
    def prepare_source_schema(self):
        """Extract schema from source database and save to SQL file."""
        try:
            from sqlalchemy import inspect
            source_engine = create_engine(
                f'postgresql://{self.db_user}:{self.db_password}@'
                f'{self.db_host}:{self.db_port}/{self.db_name}'
            )
            reserved_keywords = {
                'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric',
                'authorization', 'binary', 'both', 'case', 'cast', 'check', 'collate', 'column',
                'constraint', 'create', 'cross', 'current_catalog', 'current_date', 'current_role',
                'current_schema', 'current_time', 'current_timestamp', 'current_user', 'default',
                'deferrable', 'desc', 'distinct', 'do', 'else', 'end', 'except', 'false', 'fetch',
                'for', 'foreign', 'freeze', 'from', 'full', 'grant', 'group', 'having', 'in',
                'initially', 'inner', 'intersect', 'into', 'is', 'isnull', 'join', 'lateral',
                'leading', 'left', 'like', 'limit', 'localtime', 'localtimestamp', 'natural',
                'not', 'notnull', 'null', 'offset', 'on', 'only', 'or', 'order', 'outer', 'overlaps',
                'placing', 'primary', 'references', 'returning', 'right', 'select', 'session_user',
                'similar', 'some', 'symmetric', 'table', 'then', 'to', 'trailing', 'true', 'union',
                'unique', 'user', 'using', 'variadic', 'verbose', 'when', 'where', 'window', 'with'
            }
            def quote_ident(name):
                if name.lower() in reserved_keywords or not name.islower():
                    return f'"{name}"'
                return name
            insp = inspect(source_engine)
            tables = insp.get_table_names(schema='omopcdm')
            with open('ddl/source_schema.sql', 'w') as f:
                f.write("CREATE SCHEMA IF NOT EXISTS omopcdm;\n\n")
                for table in tables:
                    columns = insp.get_columns(table, schema='omopcdm')
                    pk = insp.get_pk_constraint(table, schema='omopcdm')
                    f.write(f"CREATE TABLE IF NOT EXISTS omopcdm.{quote_ident(table)} (\n")
                    col_lines = []
                    for col in columns:
                        colname = quote_ident(col['name'])
                        coltype = str(col['type'])
                        nullable = '' if col['nullable'] else ' NOT NULL'
                        default = f" DEFAULT {col['default']}" if col['default'] is not None else ''
                        col_lines.append(f"    {colname} {coltype}{default}{nullable}")
                    if pk and pk.get('constrained_columns'):
                        pkcols = ', '.join([quote_ident(c) for c in pk['constrained_columns']])
                        col_lines.append(f"    PRIMARY KEY ({pkcols})")
                    f.write(",\n".join(col_lines))
                    f.write("\n);\n\n")
            logging.info("Successfully created source schema SQL file")
            return True
        except Exception as e:
            logging.error(f"Error preparing source schema: {str(e)}")
            return False

    def create_partition_containers(self):
        """Create Docker containers for each partition"""
        try:
            # Clean up any existing containers first
            self.cleanup()
            
            # Create containers for each partition
            for i in range(self.num_partitions):
                port = self.port_manager.find_available_port()
                container_name = f"omop_partition_{i}"
                
                logger.info(f"Using port {port} for partition {i}")
                
                # Create container
                container = self.docker_client.containers.run(
                    "postgres:16",
                    name=container_name,
                    detach=True,
                    environment={
                        "POSTGRES_USER": "postgres",
                        "POSTGRES_PASSWORD": "postgres",
                        "POSTGRES_DB": "omop"
                    },
                    ports={'5432/tcp': port}
                )
                
                # Wait for container to be ready
                self._wait_for_container(container_name)
                
                # Create engine for the new partition with retry
                partition_url = f"postgresql://postgres:postgres@localhost:{port}/omop"
                retries = 10
                for attempt in range(retries):
                    try:
                        partition_engine = create_engine(partition_url, pool_pre_ping=True)
                        # Test the connection
                        with partition_engine.connect() as _conn:
                            _conn.execute(text("SELECT 1"))
                        break  # success
                    except Exception:
                        time.sleep(1)
                else:
                    raise RuntimeError(f"Partition {i} on port {port} failed to accept connections after {retries} seconds")
                
                # Add to partition_engines
                self.partition_engines.append((i, partition_engine))
                
                # Execute schema SQL file
                with partition_engine.connect() as conn:
                    # Set search path to include omopcdm schema
                    conn.execute(text("SET search_path TO omopcdm, public;"))
                    conn.commit()
                    
                    # Read and execute the schema SQL file
                    with open('ddl/source_schema.sql', 'r') as f:
                        sql = f.read()
                        
                    # Split into individual statements and execute each one
                    statements = sql.split(';')
                    for statement in statements:
                        statement = statement.strip()
                        if statement:  # Skip empty statements
                            try:
                                conn.execute(text(statement))
                                conn.commit()  # Commit after each statement
                            except Exception as e:
                                logger.warning(f"Error executing statement: {statement}\n{str(e)}")
                                conn.rollback()  # Rollback on error
                                continue
                
                logger.info(f"Created container for partition {i} on port {port} using PostgreSQL 16")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in create_partition_containers: {str(e)}")
            self.cleanup()  # Clean up on error
            raise
    
    def _wait_for_container(self, container_name, timeout: int = 60):
        """Wait for container to be ready to accept connections"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                container = self.docker_client.containers.get(container_name)
                if container.status == "running":
                    # Try to connect to the database
                    logs = container.logs().decode()
                    if "database system is ready to accept connections" in logs:
                        return
            except Exception:
                pass
            time.sleep(1)
        raise TimeoutError(f"Container {container_name} failed to start within {timeout} seconds")
    
    def distribute_data(self, graph: nx.DiGraph):
        """Distribute data across partitions using the selected strategy"""
        # Initialize the distribution strategy with current engines
        strategy = self.distribution_strategy(self.source_engine, self.partition_engines)
        
        # Execute the distribution
        if not strategy.distribute_data(graph):
            raise Exception("Data distribution failed")
    
    def validate_partitions(self) -> bool:
        """
        Validate that data is correctly distributed across partitions
        Returns True if validation passes, False otherwise
        """
        try:
            validation_passed = True
            
            # Get total counts from source database
            source_counts = {}
            with self.source_engine.connect() as conn:
                # Set search path for source database
                conn.execute(text("SET search_path TO omopcdm, public;"))
                conn.commit()
                
                for table_name in self.get_related_tables(self.analyze_schema()):
                    schema, table_name = table_name.split('.')
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                    source_counts[table_name] = result.scalar()
            
            # Check counts in each partition
            for i, (partition_index, engine) in enumerate(self.partition_engines):
                logger.info(f"Validating partition {partition_index}...")
                with engine.connect() as conn:
                    # Set search path for partition database
                    conn.execute(text("SET search_path TO omopcdm, public;"))
                    conn.commit()
                    
                    for table, source_count in source_counts.items():
                        schema, table_name = table.split('.')
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                        partition_count = result.scalar()
                        
                        # For tables with person_id, verify distribution
                        if self._has_person_id_column(table):
                            expected_count = source_count // self.num_partitions
                            if i < source_count % self.num_partitions:
                                expected_count += 1
                            
                            if partition_count != expected_count:
                                logger.error(f"Partition {partition_index} has incorrect count for {table}: "
                                           f"expected {expected_count}, got {partition_count}")
                                validation_passed = False
                        else:
                            # For tables without person_id, verify all rows are copied
                            if partition_count != source_count:
                                logger.error(f"Partition {partition_index} has incorrect count for {table}: "
                                           f"expected {source_count}, got {partition_count}")
                                validation_passed = False
                        
                        # Verify schema
                        result = conn.execute(text(f"""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_schema = '{schema}' 
                            AND table_name = '{table_name}'
                        """))
                        columns = {row[0]: row[1] for row in result}
                        if not columns:
                            logger.error(f"Partition {partition_index} has incorrect schema for {table}")
                            validation_passed = False
                        
                        # Verify constraints
                        result = conn.execute(text(f"""
                            SELECT constraint_name, constraint_type 
                            FROM information_schema.table_constraints 
                            WHERE table_schema = '{schema}' 
                            AND table_name = '{table_name}'
                        """))
                        constraints = {row[0]: row[1] for row in result}
                        if not constraints:
                            logger.error(f"Partition {partition_index} has incorrect constraints for {table}")
                            validation_passed = False
                        
                        # Verify data integrity (only if person_id exists)
                        if self._has_person_id_column(table):
                            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name} WHERE person_id IS NULL"))
                            null_count = result.scalar()
                            if null_count > 0:
                                logger.error(f"Partition {partition_index} has {null_count} NULL person_id values in {table}")
                                validation_passed = False
            
            # Verify that all partitions together make up the original source data
            total_partition_counts = {}
            for i, (partition_index, engine) in enumerate(self.partition_engines):
                with engine.connect() as conn:
                    # Set search path for partition database
                    conn.execute(text("SET search_path TO omopcdm, public;"))
                    conn.commit()
                    
                    for table in source_counts:
                        schema, table_name = table.split('.')
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                        count = result.scalar()
                        if table not in total_partition_counts:
                            total_partition_counts[table] = 0
                        total_partition_counts[table] += count
            
            for table, source_count in source_counts.items():
                if total_partition_counts.get(table, 0) != source_count:
                    logger.error(f"Total count for {table} across all partitions does not match source count: "
                               f"expected {source_count}, got {total_partition_counts.get(table, 0)}")
                    validation_passed = False
            
            if validation_passed:
                logger.info("All partitions validated successfully!")
            else:
                logger.error("Partition validation failed!")
            return validation_passed
            
        except Exception as e:
            logger.error(f"Error validating partitions: {str(e)}")
            return False
    
    def cleanup(self):
        """Clean up all partition containers"""
        try:
            for container in self.partition_containers:
                container.stop()
                container.remove()
            logger.info("Cleaned up all partition containers")
        except Exception as e:
            logger.error(f"Error cleaning up containers: {str(e)}")
            raise

    def analyze_partitions(self):
        """
        Analyze all partitions in detail, including row counts, tables, and other metadata.
        """
        try:
            logger.info("Analyzing partitions...")
            for i, (partition_index, engine) in enumerate(self.partition_engines):
                logger.info(f"Analyzing partition {partition_index}...")
                with engine.connect() as conn:
                    # Get list of tables in the partition
                    result = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'omopcdm'"))
                    tables = [(row[0], row[1]) for row in result]
                    logger.info(f"Partition {partition_index} has {len(tables)} tables.")
                    for schema, table in tables:
                        # Get row count for each table
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
                        count = result.scalar()
                        logger.info(f"  Table {schema}.{table}: {count} rows")
        except Exception as e:
            logger.error(f"Error analyzing partitions: {str(e)}")
            raise

    def _has_person_id_column(self, table: str) -> bool:
        schema, table_name = table.split('.')
        with self.source_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    AND column_name = 'person_id'
                );
            """))
            return result.scalar()

    def export_graph(self, graph: nx.DiGraph, filename: str, with_png: bool = True):
        """Export the graph to a file"""
        if with_png:
            nx.drawing.nx_pydot.write_dot(graph, filename + '.dot')
            os.system(f"dot -Tpng {filename}.dot -o {filename}.png")
        else:
            nx.drawing.nx_pydot.write_dot(graph, filename + '.dot')

    def export_partition_graphs(self, graph: nx.DiGraph, output_dir: str):
        """Export per-partition graphs with row counts"""
        os.makedirs(output_dir, exist_ok=True)
        for i, (partition_index, engine) in enumerate(self.partition_engines):
            logger.info(f"Exporting partition {partition_index}...")
            with engine.connect() as conn:
                # Get list of tables in the partition
                result = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'omopcdm'"))
                tables = [(row[0], row[1]) for row in result]
                logger.info(f"Partition {partition_index} has {len(tables)} tables.")
                for schema, table in tables:
                    # Get row count for each table
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
                    count = result.scalar()
                    logger.info(f"  Table {schema}.{table}: {count} rows")
            self.export_graph(graph, os.path.join(output_dir, f"partition_{partition_index}_graph"))

def calculate_expected_counts(source_engine, num_partitions: int) -> Dict[str, Dict[int, int]]:
    """Calculate expected row counts for each table in each partition."""
    expected_counts = {}
    logger.info("\n=== Theoretical Row Count Calculations ===")
    
    # Get all tables
    with source_engine.connect() as conn:
        tables = conn.execute(text("""
            SELECT table_schema || '.' || table_name as full_table_name
            FROM information_schema.tables
            WHERE table_schema = 'omopcdm'
            ORDER BY table_name;
        """)).fetchall()
        
        for table in tables:
            table_name = table[0]
            expected_counts[table_name] = {}
            
            # Special handling for episode_event
            if table_name == 'omopcdm.episode_event':
                # Get total count
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                total_rows = conn.execute(text(count_query)).scalar()
                
                logger.info(f"\nTable: {table_name}")
                logger.info(f"Total rows in source: {total_rows}")
                logger.info("Split table based on episode.person_id")
                
                # Split evenly between partitions
                base_count = total_rows // num_partitions
                remainder = total_rows % num_partitions
                
                logger.info(f"Split table calculation:")
                logger.info(f"  Base count per partition: {base_count}")
                logger.info(f"  Remainder rows: {remainder}")
                
                for i in range(num_partitions):
                    expected_count = base_count + (1 if i < remainder else 0)
                    expected_counts[table_name][i] = expected_count
                    logger.info(f"  Partition {i} expected: {expected_count}")
                continue
            
            # Get actual row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            total_rows = conn.execute(text(count_query)).scalar()
            
            # Check if table is person-dependent
            person_dep_query = f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_schema = 'omopcdm' 
                    AND table_name = '{table_name.split('.')[-1]}'
                    AND column_name = 'person_id'
                );
            """
            has_person_id = conn.execute(text(person_dep_query)).scalar()
            
            logger.info(f"\nTable: {table_name}")
            logger.info(f"Total rows in source: {total_rows}")
            logger.info(f"Person-dependent: {has_person_id}")
            
            if has_person_id:
                # Split table - divide rows evenly
                base_count = total_rows // num_partitions
                remainder = total_rows % num_partitions
                
                logger.info(f"Split table calculation:")
                logger.info(f"  Base count per partition: {base_count}")
                logger.info(f"  Remainder rows: {remainder}")
                
                for i in range(num_partitions):
                    expected_count = base_count + (1 if i < remainder else 0)
                    expected_counts[table_name][i] = expected_count
                    logger.info(f"  Partition {i} expected: {expected_count}")
            else:
                # Duplicated table - same count in all partitions
                logger.info(f"Duplicated table - same count in all partitions: {total_rows}")
                for i in range(num_partitions):
                    expected_counts[table_name][i] = total_rows
    
    logger.info("\n=== End of Theoretical Calculations ===\n")
    return expected_counts

# Add this mapping at the top of your file or near the validation function
join_partitioned_tables = {
    'omopcdm.episode_event': {
        'parent_table': 'omopcdm.episode',
        'child_key': 'episode_id',
        'parent_key': 'episode_id',
        'person_id_col': 'person_id'
    },
    # Add more join-partitioned tables here as needed
}

def get_expected_partition_count(conn, table, partition_index, num_partitions, join_partitioned_tables):
    if table in join_partitioned_tables:
        info = join_partitioned_tables[table]
        sql = f'''
            SELECT COUNT(*)
            FROM {table} c
            JOIN {info['parent_table']} p ON c.{info['child_key']} = p.{info['parent_key']}
            WHERE (p.{info['person_id_col']} % {num_partitions}) = {partition_index}
        '''
        return conn.execute(text(sql)).scalar()
    else:
        # Check if table has person_id
        schema, tbl = table.split('.')
        has_person_id = conn.execute(text(f'''
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{tbl}'
                AND column_name = 'person_id'
            );
        ''')).scalar()
        if has_person_id:
            sql = f'''
                SELECT COUNT(*) FROM {table}
                WHERE (person_id % {num_partitions}) = {partition_index}
            '''
            return conn.execute(text(sql)).scalar()
        else:
            # Duplicated table
            sql = f'SELECT COUNT(*) FROM {table}'
            return conn.execute(text(sql)).scalar()

def validate_partitions(partition_engines: List[Tuple[int, object]], source_engine: object, num_partitions: int):
    logger.info("Validating partitions...")
    with source_engine.connect() as conn:
        for partition_index, engine in partition_engines:
            logger.info(f"Validating partition {partition_index}...")
            with engine.connect() as part_conn:
                tables_query = """
                    SELECT table_schema || '.' || table_name as full_table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'omopcdm'
                    ORDER BY table_name;
                """
                tables = [row[0] for row in part_conn.execute(text(tables_query)).fetchall()]
                logger.info(f"Partition {partition_index} has {len(tables)} tables.")
                for table in tables:
                    count_query = f"SELECT COUNT(*) FROM {table}"
                    actual_count = part_conn.execute(text(count_query)).scalar()
                    expected_count = get_expected_partition_count(conn, table, partition_index, num_partitions, join_partitioned_tables)
                    if actual_count != expected_count:
                        logger.error(f"Partition {partition_index} has incorrect count for {table}: "
                                   f"expected {expected_count}, got {actual_count}")
                        raise Exception(f"Partition validation failed for {table}")
                    else:
                        logger.info(f"  Table {table}: {actual_count} rows (expected: {expected_count})")

def main():
    """Main function to run the partitioner"""
    try:
        load_dotenv()
        
        # Get database connection details from environment variables
        source_db_url = os.getenv('SOURCE_DB_URL')
        num_partitions = int(os.getenv('NUM_PARTITIONS', '2'))
        distribution_strategy = os.getenv('DISTRIBUTION_STRATEGY', 'uniform')
        
        if not source_db_url:
            raise ValueError("SOURCE_DB_URL environment variable is required")
        
        # Parse the source database URL to get the port
        parsed_url = urlparse(source_db_url)
        source_port = parsed_url.port or 5432
        logger.info(f"Using source database port: {source_port}")
        
        # Initialize partitioner
        partitioner = OMOPPartitioner(source_db_url, num_partitions, distribution_strategy)
        
        # Prepare schema from source database
        partitioner.prepare_source_schema()
        
        # Create partition containers
        partitioner.create_partition_containers()
        
        # Analyze schema and create source graph (shows table relationships from source database)
        graph = partitioner.analyze_schema()
        os.makedirs("output", exist_ok=True)
        partitioner.export_graph(graph, "output/source_graph", with_png=True)
        
        # Distribute data
        partitioner.distribute_data(graph)
        
        # Export per-partition graphs with row counts
        partitioner.export_partition_graphs(graph, output_dir="output")
        
        # Validate partitions with theoretical checks
        validate_partitions(partitioner.partition_engines, partitioner.source_engine, num_partitions)
        
        logger.info("Partitioning completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise
    finally:
        # Note: Cleanup is now handled separately
        pass

if __name__ == "__main__":
    main() 
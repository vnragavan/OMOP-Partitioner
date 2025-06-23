from abc import ABC, abstractmethod
from typing import List, Dict, Set, Tuple
import networkx as nx
from sqlalchemy import create_engine, text, inspect
import logging
import tempfile
import os

logger = logging.getLogger(__name__)

class DistributionStrategy(ABC):
    """Base class for distribution strategies"""
    
    def __init__(self, source_engine, partition_engines: List[tuple]):
        self.source_engine = source_engine
        self.partition_engines = partition_engines
        self.person_table = 'omopcdm.person'
        self.schema = 'omopcdm'
        # Will be set by concrete distribute_data to know FK graph
        self.dependency_graph = None
    
    @abstractmethod
    def distribute_data(self, graph: nx.DiGraph) -> bool:
        """Distribute data across partitions"""
        pass
    
    def get_related_tables(self, graph: nx.DiGraph) -> List[str]:
        """
        Get all tables related to the person table through foreign key relationships
        Returns tables in order of their dependency (tables with no dependencies first)
        """
        # Get all nodes that have a path to person table
        related_nodes = set()
        for node in graph.nodes():
            if node != self.person_table and nx.has_path(graph, node, self.person_table):
                related_nodes.add(node)
        related_nodes.add(self.person_table)
        
        # Create a subgraph with only related nodes
        subgraph = graph.subgraph(related_nodes)
        
        # Get topological sort of the subgraph
        try:
            return list(nx.topological_sort(subgraph))
        except nx.NetworkXUnfeasible:
            # If there's a cycle, fall back to simple list
            return list(related_nodes)

    def _has_person_id_column(self, table: str) -> bool:
        """Check if a table has a person_id column"""
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

    def _bulk_copy(self, table: str, select_query: str, dest_engine):
        """Perform COPY (select_query) TO / FROM between source and dest using temp file."""
        schema, table_name = table.split('.')
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_path = tmp_file.name
        tmp_file.close()
        
        # COPY out from source
        src_conn = self.source_engine.raw_connection()
        try:
            with open(tmp_path, 'wb') as out_f:
                cur = src_conn.cursor()
                cur.copy_expert(f"COPY ({select_query}) TO STDOUT BINARY", out_f)
                src_conn.commit()
        finally:
            src_conn.close()
            
        # Size for logging
        file_size = os.path.getsize(tmp_path)
        
        # COPY in to destination
        # Handle both engine object and (index, engine) tuple
        if isinstance(dest_engine, tuple):
            dest_engine = dest_engine[1]  # Get the engine from the tuple
            
        dest_conn = dest_engine.raw_connection()
        try:
            cur = dest_conn.cursor()
            # Ensure search_path
            cur.execute("SET search_path TO omopcdm, public;")
            with open(tmp_path, 'rb') as in_f:
                cur.copy_expert(f"COPY {schema}.{table_name} FROM STDIN BINARY", in_f)
            dest_conn.commit()
        finally:
            dest_conn.close()
            os.remove(tmp_path)
            
        logger.info(f"Bulk-copied {file_size} bytes into partition {dest_engine.url} for table {table}")

    def _copy_table_data(self, table: str) -> None:
        """Copy data from source to all partitions using bulk COPY."""
        if not self.partition_engines:
            logger.error("No partition engines available for data distribution. Skipping table: %s", table)
            return
            
        schema, table_name = table.split('.')
        
        # List of known lookup/reference tables that should be duplicated
        lookup_tables = {
            'concept', 'concept_ancestor', 'concept_class', 'concept_relationship',
            'concept_synonym', 'domain', 'drug_strength', 'relationship',
            'source_to_concept_map', 'vocabulary', 'cdm_source'
        }
        
        # Get total count of rows
        with self.source_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            total_rows = result.scalar()
            
        if total_rows == 0:
            logger.info(f"No rows to distribute for table {schema}.{table_name}")
            return
            
        # Special handling for episode_event
        if table_name == 'episode_event':
            for partition_index, engine in enumerate(self.partition_engines):
                select_query = f"""
                    SELECT ee.* 
                    FROM {schema}.{table_name} ee
                    JOIN {schema}.episode e ON ee.episode_id = e.episode_id
                    WHERE (e.person_id % {len(self.partition_engines)}) = {partition_index}
                """
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Split {schema}.{table_name} to partition {partition_index} based on episode.person_id")
            return
            
        # For lookup tables, copy full table to every partition
        if table_name in lookup_tables:
            for partition_index, engine in enumerate(self.partition_engines):
                select_query = f"SELECT * FROM {schema}.{table_name}"
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Copied full {schema}.{table_name} to partition {partition_index}")
            return
            
        # For person-dependent tables, use modulus on person_id
        has_person = self._has_person_id_column(table)
        if has_person:
            for partition_index, engine in enumerate(self.partition_engines):
                select_query = f"SELECT * FROM {schema}.{table_name} WHERE (person_id % {len(self.partition_engines)}) = {partition_index}"
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Distributed rows of {schema}.{table_name} to partition {partition_index} using modulus on person_id")
            return
            
        # For any other tables, copy full table to every partition
        for partition_index, engine in enumerate(self.partition_engines):
            select_query = f"SELECT * FROM {schema}.{table_name}"
            self._bulk_copy(table, select_query, engine)
            logger.info(f"Copied full {schema}.{table_name} to partition {partition_index}")

    # ---------------- helper for large lookup tables -----------------
    def _get_hash_column(self, table: str) -> str | None:
        """Return a column that can be used for hash-partitioning (concept_id or similar)."""
        try:
            schema, table_name = table.split('.')
            insp = inspect(self.source_engine)
            cols = insp.get_columns(table_name, schema=schema)
            for col in cols:
                if 'concept_id' in col['name']:
                    return col['name']
            return None
        except Exception:
            return None

    def _is_person_dependent(self, table: str) -> bool:
        """Check if a table is person-dependent."""
        schema, table_name = table.split('.')
        
        # List of known lookup/reference tables that should be duplicated
        lookup_tables = {
            'concept', 'concept_ancestor', 'concept_class', 'concept_relationship',
            'concept_synonym', 'domain', 'drug_strength', 'relationship',
            'source_to_concept_map', 'vocabulary', 'cdm_source'
        }
        
        if table_name in lookup_tables:
            return False
            
        # Check if table has person_id column
        with self.source_engine.connect() as conn:
            has_person_id = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    AND column_name = 'person_id'
                );
            """)).scalar()
            
            if has_person_id:
                return True
                
            # Check for indirect person dependency through episode
            if table_name == 'episode_event':
                return True
                
            return False

    def distribute_table(self, table: str, total_rows: int):
        """Distribute a table's data across partitions."""
        schema, table_name = table.split('.')
        
        # List of known lookup/reference tables that should be duplicated
        lookup_tables = {
            'concept', 'concept_ancestor', 'concept_class', 'concept_relationship',
            'concept_synonym', 'domain', 'drug_strength', 'relationship',
            'source_to_concept_map', 'vocabulary', 'cdm_source'
        }
        
        # Special handling for episode_event - split based on episode's person_id
        if table == 'omopcdm.episode_event':
            for partition_index, engine in enumerate(self.partition_engines):
                select_query = f"""
                    SELECT ee.* 
                    FROM {schema}.{table_name} ee
                    JOIN {schema}.episode e ON ee.episode_id = e.episode_id
                    WHERE (e.person_id % {len(self.partition_engines)}) = {partition_index}
                """
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Split {schema}.{table_name} to partition {partition_index} based on episode.person_id")
            return

        # For lookup tables, copy full table to every partition
        if table_name in lookup_tables:
            for partition_index, engine in enumerate(self.partition_engines):
                select_query = f"SELECT * FROM {schema}.{table_name}"
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Copied full {schema}.{table_name} to partition {partition_index}")
            return

        # For person-dependent tables, split based on person_id
        if self._is_person_dependent(table):
            rows_per_partition = total_rows // len(self.partition_engines)
            remainder = total_rows % len(self.partition_engines)
            
            for partition_index, engine in enumerate(self.partition_engines):
                limit = rows_per_partition + (1 if partition_index < remainder else 0)
                offset = partition_index * rows_per_partition + min(partition_index, remainder)
                select_query = f"SELECT * FROM {schema}.{table_name} ORDER BY person_id LIMIT {limit} OFFSET {offset}"
                self._bulk_copy(table, select_query, engine)
                logger.info(f"Distributed {total_rows} rows of {schema}.{table_name} to partition {partition_index}")
            return

        # For any other tables, copy full table to every partition
        for partition_index, engine in enumerate(self.partition_engines):
            select_query = f"SELECT * FROM {schema}.{table_name}"
            self._bulk_copy(table, select_query, engine)
            logger.info(f"Copied full {schema}.{table_name} to partition {partition_index}")

class UniformDistributionStrategy(DistributionStrategy):
    """Distributes data uniformly across partitions based on person_id ranges"""
    
    def __init__(self, source_engine, partition_engines: List[tuple]):
        super().__init__(source_engine, partition_engines)
        self.num_partitions = len(partition_engines)
    
    def distribute_data(self, graph: nx.DiGraph) -> bool:
        """Distribute data uniformly across partitions."""
        try:
            # Keep graph reference for _copy_table_data
            self.dependency_graph = graph

            # Get all tables from the source database
            with self.source_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'omopcdm'
                """))
                tables = [f"{self.schema}.{row[0]}" for row in result]
            
            # Distribute data for each table
            for table in tables:
                try:
                    # Skip table creation since tables are already created by the SQL file
                    # Just copy the data
                    self._copy_table_data(table)
                except Exception as e:
                    logging.error(f"Error distributing data for table {table}: {str(e)}")
                    continue
            
            return True
        except Exception as e:
            logging.error(f"Error in uniform distribution: {str(e)}")
            return False

class HashDistributionStrategy(DistributionStrategy):
    """Distributes data using hash-based partitioning"""
    
    def distribute_data(self, graph: nx.DiGraph) -> bool:
        """Distribute data using hash-based partitioning"""
        try:
            related_tables = self.get_related_tables(graph)
            num_partitions = len(self.partition_engines)
            
            # Get all person IDs
            with self.source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT person_id FROM {self.person_table}"))
                all_person_ids = [row[0] for row in result]
            
            # Distribute person IDs using hash
            partition_person_ids = [[] for _ in range(num_partitions)]
            for person_id in all_person_ids:
                partition_index = hash(str(person_id)) % num_partitions
                partition_person_ids[partition_index].append(person_id)
            
            # Copy data for each partition
            for partition_index, person_ids in enumerate(partition_person_ids):
                for table in related_tables:
                    self._copy_table_data(table, person_ids, partition_index)
                
                logger.info(f"Distributed data for partition {partition_index}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in hash distribution: {str(e)}")
            return False

class RoundRobinDistributionStrategy(DistributionStrategy):
    """Distributes data using round-robin partitioning"""
    
    def distribute_data(self, graph: nx.DiGraph) -> bool:
        """Distribute data using round-robin partitioning"""
        try:
            related_tables = self.get_related_tables(graph)
            num_partitions = len(self.partition_engines)
            
            # Get all person IDs
            with self.source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT person_id FROM {self.person_table}"))
                all_person_ids = [row[0] for row in result]
            
            # Distribute person IDs using round-robin
            partition_person_ids = [[] for _ in range(num_partitions)]
            for i, person_id in enumerate(all_person_ids):
                partition_index = i % num_partitions
                partition_person_ids[partition_index].append(person_id)
            
            # Copy data for each partition
            for partition_index, person_ids in enumerate(partition_person_ids):
                for table in related_tables:
                    self._copy_table_data(table, person_ids, partition_index)
                
                logger.info(f"Distributed data for partition {partition_index}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in round-robin distribution: {str(e)}")
            return False

    def distribute_data(self, graph: nx.DiGraph) -> bool:
        """Distribute data across partitions based on the strategy."""
        try:
            # 1. Create schema and tables in each partition using the SQL file
            for partition_engine in self.partition_engines:
                with partition_engine.connect() as conn:
                    with open('ddl/remote_db_structure.sql', 'r') as f:
                        sql = f.read()
                        for statement in sql.split(';'):
                            stmt = statement.strip()
                            if stmt:
                                try:
                                    conn.execute(text(stmt))
                                except Exception as e:
                                    # Ignore errors for statements like CREATE SCHEMA if already exists
                                    if 'already exists' not in str(e):
                                        logging.warning(f"Error executing statement: {stmt[:50]}...\n{e}")
                    conn.commit()

            # 2. Get all tables in the source database
            with self.source_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'omopcdm'
                """))
                tables = [row[0] for row in result]

            # 3. Distribute data for each table (do NOT create tables again)
            for table in tables:
                self._distribute_table_data(table)

            return True
        except Exception as e:
            logging.error(f"Error in {self.__class__.__name__} distribution: {str(e)}")
            return False 
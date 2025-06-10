import os
import docker
from tabulate import tabulate
from dotenv import load_dotenv
from config_manager import ConfigManager

def show_partition_info():
    """Display information about all created partitions and save to YAML"""
    client = docker.from_env()
    config_manager = ConfigManager()
    
    # Get all partition containers
    containers = [c for c in client.containers.list() if c.name.startswith('omop_partition_')]
    
    if not containers:
        print("No partition containers found. Run omop_partitioner.py first.")
        return
    
    # Prepare table data and config info
    table_data = []
    config_info = []
    source_db_url = os.getenv('SOURCE_DB_URL', 'N/A')
    
    for container in containers:
        # Get container info
        container.reload()
        ports = container.ports.get('5432/tcp', [])
        port = ports[0]['HostPort'] if ports else 'N/A'
        
        # Get environment variables
        env = container.attrs['Config']['Env']
        db_name = next((v.split('=')[1] for v in env if v.startswith('POSTGRES_DB=')), 'N/A')
        username = next((v.split('=')[1] for v in env if v.startswith('POSTGRES_USER=')), 'N/A')
        password = next((v.split('=')[1] for v in env if v.startswith('POSTGRES_PASSWORD=')), 'N/A')
        
        # Get container status
        status = container.status
        
        # Prepare connection string
        connection_string = f"postgresql://{username}:{password}@localhost:{port}/{db_name}"
        
        # Add to table data
        table_data.append([
            container.name,
            db_name,
            port,
            username,
            password,
            status
        ])
        
        # Add to config info
        config_info.append({
            "container_name": container.name,
            "status": status,
            "port": port,
            "db_name": db_name,
            "username": username,
            "password": password,
            "connection_string": connection_string
        })
    
    # Print table
    headers = ['Container', 'Database', 'Port', 'Username', 'Password', 'Status']
    print("\nPartition Information:")
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # Print connection strings
    print("\nConnection Strings:")
    for row in table_data:
        print(f"\n{row[0]}:")
        print(f"postgresql://{row[3]}:{row[4]}@localhost:{row[2]}/{row[1]}")
    
    # Save configuration to YAML
    config_file = config_manager.save_partition_config(config_info, source_db_url)
    print(f"\nConfiguration saved to: {config_file}")

if __name__ == "__main__":
    load_dotenv()
    show_partition_info() 
import yaml
import os
from datetime import datetime
from typing import Dict, List

class ConfigManager:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
    
    def save_partition_config(self, partition_info: List[Dict], source_db_url: str):
        """Save partition configuration to YAML file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_file = os.path.join(self.config_dir, f"partition_config_{timestamp}.yaml")
        
        config = {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "postgres_version": "16",
                "source_database": source_db_url
            },
            "partitions": []
        }
        
        for info in partition_info:
            partition_config = {
                "container": {
                    "name": info["container_name"],
                    "status": info["status"],
                    "port": info["port"]
                },
                "database": {
                    "name": info["db_name"],
                    "username": info["username"],
                    "password": info["password"],
                    "connection_string": info["connection_string"]
                },
                "postgres": {
                    "version": "16",
                    "port": info["port"],
                    "host": "localhost"
                }
            }
            config["partitions"].append(partition_config)
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        return config_file 
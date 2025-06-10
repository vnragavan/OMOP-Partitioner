import os
import sys
import subprocess
import platform
import shutil
import venv
from typing import List, Tuple
from pathlib import Path
import re
import time
import importlib
import json
import socket

class DependencyChecker:
    def __init__(self):
        self.system = platform.system().lower()
        self.python_version = sys.version_info
        self.required_python_version = (3, 8)
        self.required_packages = [
            'sqlalchemy>=2.0.0',
            'psycopg2-binary>=2.9.0',
            'docker>=6.0.0',
            'pandas>=2.0.0',
            'networkx>=3.0.0',
            'tqdm>=4.65.0',
            'python-dotenv>=1.0.0',
            'tabulate>=0.9.0',
            'PyYAML>=6.0.0'
        ]
        self.venv_dir = Path('.venv')
        self.venv_python = self.venv_dir / 'bin' / 'python' if self.system != 'windows' else self.venv_dir / 'Scripts' / 'python.exe'
        self.venv_pip = self.venv_dir / 'bin' / 'pip' if self.system != 'windows' else self.venv_dir / 'Scripts' / 'pip.exe'
        self.env_file = Path('.env')
        self.db_name = 'omop_db'
        self.db_user = 'postgres'
        self.db_password = 'postgres'
        self.db_port = self._find_available_postgres_port()
        self.db_version = '16'
        # Default environment configuration
        self.default_env_config = {
            'SOURCE_DB_URL': f'postgresql://{self.db_user}:{self.db_password}@localhost:{self.db_port}/{self.db_name}',
            'NUM_PARTITIONS': '2',
            'DISTRIBUTION_STRATEGY': 'uniform',  # Options: uniform, hash, round_robin
            'POSTGRES_VERSION': self.db_version,
            'POSTGRES_USER': self.db_user,
            'POSTGRES_PASSWORD': self.db_password,
            'POSTGRES_DB': self.db_name,
            'DOCKER_NETWORK': 'omop_network',
            'CONFIG_FILE': 'partitions_config.yaml'
        }
    
    def _find_available_postgres_port(self) -> int:
        """Find an available PostgreSQL port starting from 5432"""
        import socket
        import subprocess
        import time
        
        def is_postgres_running(port: int) -> bool:
            """Check if PostgreSQL is actually running on the port"""
            try:
                # Try to connect to PostgreSQL
                result = subprocess.run(
                    ['psql', '-h', 'localhost', '-p', str(port), '-U', 'postgres', '-c', 'SELECT 1'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return result.returncode == 0
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                return False
        
        # First try the default PostgreSQL port
        if self._is_port_available(5432) and is_postgres_running(5432):
            print("Using default PostgreSQL port 5432")
            return 5432
            
        # Try ports in a reasonable range
        for port in range(5432, 5532):
            if self._is_port_available(port) and is_postgres_running(port):
                print(f"Using alternative PostgreSQL port {port}")
                return port
                
        # If no running PostgreSQL instance is found, try to start one on the default port
        print("No running PostgreSQL instance found. Attempting to start PostgreSQL...")
        if self._is_macos():
            subprocess.run(['brew', 'services', 'restart', 'postgresql@16'])
            time.sleep(5)  # Give PostgreSQL time to start
            if is_postgres_running(5432):
                print("Successfully started PostgreSQL on port 5432")
                return 5432
        
        raise RuntimeError("No available PostgreSQL instance found. Please ensure PostgreSQL is running.")

    def _is_port_available(self, port: int) -> bool:
        """Check if a port is available by attempting to bind to it"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return True
            except socket.error:
                return False
    
    def validate_db_url(self, url: str) -> bool:
        """Validate PostgreSQL connection URL format"""
        pattern = r'^postgresql://[^:]+:[^@]+@[^:]+:\d+/[^/]+$'
        return bool(re.match(pattern, url))
    
    def create_env_file(self) -> bool:
        """Create .env file with default configuration"""
        try:
            if self.env_file.exists():
                print(f"Found existing .env file at {self.env_file}")
                # Read existing configuration
                existing_config = {}
                with open(self.env_file, 'r') as f:
                    for line in f:
                        if '=' in line:
                            key, value = line.strip().split('=', 1)
                            existing_config[key] = value
                
                # Update default config with existing values
                self.default_env_config.update(existing_config)
                
                # Always update the port in the database URL
                self.default_env_config['SOURCE_DB_URL'] = f'postgresql://{self.db_user}:{self.db_password}@localhost:{self.db_port}/{self.db_name}'
            
            # Create or update .env file
            with open(self.env_file, 'w') as f:
                f.write("# OMOP Partitioner Configuration\n\n")
                f.write("# Database Configuration\n")
                f.write(f"SOURCE_DB_URL={self.default_env_config['SOURCE_DB_URL']}\n")
                f.write(f"POSTGRES_VERSION={self.default_env_config['POSTGRES_VERSION']}\n")
                f.write(f"POSTGRES_USER={self.default_env_config['POSTGRES_USER']}\n")
                f.write(f"POSTGRES_PASSWORD={self.default_env_config['POSTGRES_PASSWORD']}\n")
                f.write(f"POSTGRES_DB={self.default_env_config['POSTGRES_DB']}\n\n")
                
                f.write("# Partitioning Configuration\n")
                f.write(f"NUM_PARTITIONS={self.default_env_config['NUM_PARTITIONS']}\n")
                f.write(f"DISTRIBUTION_STRATEGY={self.default_env_config['DISTRIBUTION_STRATEGY']}\n\n")
                
                f.write("# Docker Configuration\n")
                f.write(f"DOCKER_NETWORK={self.default_env_config['DOCKER_NETWORK']}\n\n")
                
                f.write("# Output Configuration\n")
                f.write(f"CONFIG_FILE={self.default_env_config['CONFIG_FILE']}\n")
            
            print(f"Created/Updated .env file at {self.env_file}")
            print(f"Using PostgreSQL port: {self.db_port}")
            
            return True
        except Exception as e:
            print(f"Error creating .env file: {str(e)}")
            return False
    
    def validate_env_config(self) -> bool:
        """Validate the environment configuration"""
        try:
            if not self.env_file.exists():
                print("Error: .env file not found")
                return False
            
            # Read and validate configuration
            with open(self.env_file, 'r') as f:
                config = {}
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        config[key] = value
            
            # Check required fields
            required_fields = [
                'SOURCE_DB_URL',
                'NUM_PARTITIONS',
                'DISTRIBUTION_STRATEGY',
                'POSTGRES_VERSION',
                'POSTGRES_USER',
                'POSTGRES_PASSWORD',
                'POSTGRES_DB'
            ]
            
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                print(f"Error: Missing required fields in .env file: {', '.join(missing_fields)}")
                return False
            
            # Validate database URL
            if not self.validate_db_url(config['SOURCE_DB_URL']):
                print("Error: Invalid SOURCE_DB_URL format")
                print("Expected format: postgresql://username:password@host:port/database")
                return False
            
            # Validate number of partitions
            try:
                num_partitions = int(config['NUM_PARTITIONS'])
                if num_partitions < 1:
                    print("Error: NUM_PARTITIONS must be greater than 0")
                    return False
            except ValueError:
                print("Error: NUM_PARTITIONS must be a positive integer")
                return False
            
            # Validate distribution strategy
            valid_strategies = ['uniform', 'hash', 'round_robin']
            if config['DISTRIBUTION_STRATEGY'] not in valid_strategies:
                print(f"Error: Invalid DISTRIBUTION_STRATEGY. Must be one of: {', '.join(valid_strategies)}")
                return False
            
            return True
        except Exception as e:
            print(f"Error validating .env configuration: {str(e)}")
            return False
    
    def create_virtual_environment(self) -> bool:
        """Create a virtual environment if it doesn't exist"""
        try:
            if not self.venv_dir.exists():
                print("Creating virtual environment...")
                venv.create(self.venv_dir, with_pip=True)
                print(f"Virtual environment created at {self.venv_dir}")
            else:
                print(f"Virtual environment already exists at {self.venv_dir}")
            return True
        except Exception as e:
            print(f"Error creating virtual environment: {str(e)}")
            return False
    
    def activate_virtual_environment(self) -> bool:
        """Activate the virtual environment"""
        try:
            if self.system == 'windows':
                activate_script = self.venv_dir / 'Scripts' / 'activate.bat'
                if not activate_script.exists():
                    print("Error: Virtual environment activation script not found")
                    return False
                os.environ['VIRTUAL_ENV'] = str(self.venv_dir)
                os.environ['PATH'] = str(self.venv_dir / 'Scripts') + os.pathsep + os.environ['PATH']
            else:
                activate_script = self.venv_dir / 'bin' / 'activate'
                if not activate_script.exists():
                    print("Error: Virtual environment activation script not found")
                    return False
                os.environ['VIRTUAL_ENV'] = str(self.venv_dir)
                os.environ['PATH'] = str(self.venv_dir / 'bin') + os.pathsep + os.environ['PATH']
            
            # Set PYTHONPATH to include the virtual environment's site-packages
            site_packages = self.venv_dir / 'lib' / f'python{sys.version_info.major}.{sys.version_info.minor}' / 'site-packages'
            if self.system == 'windows':
                site_packages = self.venv_dir / 'Lib' / 'site-packages'
            
            if site_packages.exists():
                os.environ['PYTHONPATH'] = str(site_packages)
            
            print("Virtual environment activated")
            return True
        except Exception as e:
            print(f"Error activating virtual environment: {str(e)}")
            return False
    
    def upgrade_pip(self) -> bool:
        """Upgrade pip in the virtual environment"""
        try:
            subprocess.check_call([str(self.venv_python), '-m', 'pip', 'install', '--upgrade', 'pip'])
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error upgrading pip: {str(e)}")
            return False
    
    def check_python_version(self) -> bool:
        """Check if Python version meets requirements"""
        if self.python_version < self.required_python_version:
            print(f"Error: Python {self.required_python_version[0]}.{self.required_python_version[1]} or higher is required")
            print(f"Current version: {self.python_version[0]}.{self.python_version[1]}")
            return False
        return True
    
    def check_docker(self) -> Tuple[bool, str]:
        """Check if Docker is installed and running"""
        try:
            # Check Docker installation
            if not shutil.which('docker'):
                return False, "Docker is not installed"
            
            # Get Docker version
            version_output = self._run_command("docker --version", capture_output=True)
            if version_output:
                print(f"\n=== Docker Information ===")
                print(f"Docker Version: {version_output}")
                
                # Get Docker system info
                system_info = self._run_command("docker info", capture_output=True)
                if system_info:
                    print("\nDocker System Information:")
                    print(system_info)
            
            # Check if Docker daemon is running
            try:
                subprocess.run(['docker', 'info'], check=True, capture_output=True)
                return True, "Docker is installed and running"
            except subprocess.CalledProcessError:
                return False, "Docker is installed but not running"
        except Exception as e:
            return False, f"Error checking Docker: {str(e)}"
    
    def check_postgres(self) -> Tuple[bool, str]:
        """Check PostgreSQL installation and initialize if needed"""
        print("\n=== PostgreSQL Information ===")
        
        # Check if PostgreSQL is installed
        if not self._run_command("which postgres"):
            print("PostgreSQL is not installed. Installing PostgreSQL 16...")
            if self._is_macos():
                self._run_command("brew install postgresql@16")
                self._run_command("brew services start postgresql@16")
            elif self._is_linux():
                self._run_command("sudo apt-get update")
                self._run_command("sudo apt-get install -y postgresql-16")
            elif self._is_windows():
                print("Please install PostgreSQL 16 manually from https://www.postgresql.org/download/windows/")
                return False, "PostgreSQL not installed"
        else:
            # Get and print PostgreSQL version
            version_output = self._run_command("postgres --version", capture_output=True)
            if version_output:
                print(f"PostgreSQL Version: {version_output}")

        # Get PostgreSQL version
        version_output = self._run_command("postgres --version", capture_output=True)
        if version_output and "16" not in version_output:
            print("\nPostgreSQL 16 is not installed or not the default version.")
            print("Installing PostgreSQL 16...")
            if self._is_macos():
                self._run_command("brew install postgresql@16")
                self._run_command("brew services start postgresql@16")
            elif self._is_linux():
                self._run_command("sudo apt-get update")
                self._run_command("sudo apt-get install -y postgresql-16")
            elif self._is_windows():
                print("Please install PostgreSQL 16 manually from https://www.postgresql.org/download/windows/")
                return False, "PostgreSQL 16 not installed"

        # Check if data directory is initialized
        data_dir = "/opt/homebrew/var/postgresql@16" if self._is_macos() else "/var/lib/postgresql/16/main"
        if not os.path.exists(os.path.join(data_dir, "PG_VERSION")):
            print("\nInitializing PostgreSQL data directory...")
            if self._is_macos():
                self._run_command(f"initdb -D {data_dir}")
            elif self._is_linux():
                self._run_command("sudo -u postgres /usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/16/main")

        # Ensure PostgreSQL service is running
        if self._is_macos():
            self._run_command("brew services restart postgresql@16")
        elif self._is_linux():
            self._run_command("sudo systemctl restart postgresql")

        # Wait for PostgreSQL to start
        time.sleep(5)  # Give PostgreSQL time to start

        # Check if postgres role exists
        print("\n=== PostgreSQL Role Check ===")
        role_check = self._run_command("psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='postgres'\"", capture_output=True)
        if not role_check or role_check.strip() != "1":
            print("Creating PostgreSQL superuser...")
            if self._is_macos():
                self._run_command("createuser -s postgres")
            elif self._is_linux():
                self._run_command("sudo -u postgres createuser -s postgres")
        else:
            print("PostgreSQL role 'postgres' already exists.")

        # Check if database exists
        print("\n=== Database Check ===")
        db_check = self._run_command("psql -U postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='omop_db'\"", capture_output=True)
        if not db_check or db_check.strip() != "1":
            print("Creating database...")
            if self._is_macos():
                self._run_command("createdb -U postgres omop_db")
            elif self._is_linux():
                self._run_command("sudo -u postgres createdb omop_db")
        else:
            print("Database 'omop_db' already exists.")

        # Ensure privileges are granted (this is safe to run even if already granted)
        print("\n=== Setting Database Privileges ===")
        if self._is_macos():
            self._run_command("psql -U postgres -d omop_db -c 'GRANT ALL PRIVILEGES ON DATABASE omop_db TO postgres;'")
            self._run_command("psql -U postgres -d omop_db -c 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;'")
            self._run_command("psql -U postgres -d omop_db -c 'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;'")
        elif self._is_linux():
            self._run_command("sudo -u postgres psql -d omop_db -c 'GRANT ALL PRIVILEGES ON DATABASE omop_db TO postgres;'")
            self._run_command("sudo -u postgres psql -d omop_db -c 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;'")
            self._run_command("sudo -u postgres psql -d omop_db -c 'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;'")

        print("\nPostgreSQL setup completed successfully.")
        return True, "PostgreSQL setup completed successfully"

    def _run_command(self, command, capture_output=False):
        """Run a shell command and return its output if capture_output is True"""
        try:
            if capture_output:
                result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
                return result.stdout.strip()
            else:
                subprocess.run(command, shell=True, check=True)
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {command}")
            print(f"Error: {str(e)}")
            return False

    def _is_macos(self) -> bool:
        """Check if the system is macOS"""
        return self.system == 'darwin'

    def _is_linux(self) -> bool:
        """Check if the system is Linux"""
        return self.system == 'linux'

    def _is_windows(self) -> bool:
        """Check if the system is Windows"""
        return self.system == 'windows'

    def check_python_packages(self) -> List[str]:
        """Check if required Python packages are installed in the virtual environment"""
        print("\n=== Python Package Information ===")
        print(f"Python Version: {platform.python_version()}")
        
        required_packages = [
            'sqlalchemy',
            'psycopg2-binary',
            'docker',
            'pandas',
            'networkx',
            'tqdm',
            'python-dotenv',
            'tabulate',
            'PyYAML'
        ]
        
        missing_packages = []
        
        # Get all installed packages and their versions
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'pip', 'list', '--format=json'],
                capture_output=True,
                text=True,
                check=True
            )
            installed_packages = {pkg['name'].lower(): pkg['version'] for pkg in json.loads(result.stdout)}
        except subprocess.CalledProcessError as e:
            print(f"Error getting installed packages: {str(e)}")
            return required_packages

        print("\nInstalled Package Versions:")
        for package in required_packages:
            package_lower = package.lower()
            if package_lower in installed_packages:
                print(f"{package}: {installed_packages[package_lower]}")
            else:
                missing_packages.append(package)
                print(f"{package}: Not installed")

        if missing_packages:
            print(f"\nInstalling missing packages: {missing_packages}")
            try:
                # Upgrade pip first
                subprocess.run([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'], check=True)
                
                # Install missing packages
                for package in missing_packages:
                    print(f"Installing {package}...")
                    subprocess.run([sys.executable, '-m', 'pip', 'install', package], check=True)
                    print(f"Successfully installed {package}")
                
                # Verify installation again
                result = subprocess.run(
                    [sys.executable, '-m', 'pip', 'list', '--format=json'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                installed_packages = {pkg['name'].lower(): pkg['version'] for pkg in json.loads(result.stdout)}
                
                print("\n=== Python Package Information ===")
                print(f"Python Version: {platform.python_version()}")
                print("\nInstalled Package Versions:")
                for package in required_packages:
                    package_lower = package.lower()
                    if package_lower in installed_packages:
                        print(f"{package}: {installed_packages[package_lower]}")
                    else:
                        print(f"{package}: Not installed")
                        missing_packages.append(package)
                
                if missing_packages:
                    print(f"Error: Failed to install packages: {missing_packages}")
                    return missing_packages
                
            except subprocess.CalledProcessError as e:
                print(f"Error installing packages: {str(e)}")
                return missing_packages

        return missing_packages
    
    def install_python_packages(self, packages: List[str]) -> bool:
        """Install missing Python packages in the virtual environment"""
        try:
            # First upgrade pip
            self.run_subprocess([str(self.venv_python), '-m', 'pip', 'install', '--upgrade', 'pip'])
            
            # Install packages
            for package in packages:
                print(f"Installing {package}...")
                result = self.run_subprocess([str(self.venv_pip), 'install', package], capture_output=True)
                if result and result.returncode != 0:
                    print(f"Error installing {package}: {result.stderr}")
                    return False
                print(f"Successfully installed {package}")
            return True
        except Exception as e:
            print(f"Error installing packages: {str(e)}")
            return False
    
    def install_docker(self) -> bool:
        """Install Docker based on the operating system"""
        try:
            if self.system == 'linux':
                # For Ubuntu/Debian
                if shutil.which('apt-get'):
                    subprocess.run(['sudo', 'apt-get', 'update'])
                    subprocess.run(['sudo', 'apt-get', 'install', '-y', 'docker.io'])
                # For CentOS/RHEL
                elif shutil.which('yum'):
                    subprocess.run(['sudo', 'yum', 'install', '-y', 'docker'])
                # For Fedora
                elif shutil.which('dnf'):
                    subprocess.run(['sudo', 'dnf', 'install', '-y', 'docker'])
            
            elif self.system == 'darwin':  # macOS
                print("Please install Docker Desktop for Mac from https://www.docker.com/products/docker-desktop")
                return False
            
            elif self.system == 'windows':
                print("Please install Docker Desktop for Windows from https://www.docker.com/products/docker-desktop")
                return False
            
            # Start Docker service
            if self.system == 'linux':
                subprocess.run(['sudo', 'systemctl', 'start', 'docker'])
                subprocess.run(['sudo', 'systemctl', 'enable', 'docker'])
            
            return True
        except Exception as e:
            print(f"Error installing Docker: {str(e)}")
            return False
    
    def check_disk_space(self, required_space_gb: int = 10) -> Tuple[bool, str]:
        """Check if there's enough disk space"""
        try:
            if self.system == 'windows':
                import ctypes
                free_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    ctypes.c_wchar_p(os.getcwd()), None, None, ctypes.pointer(free_bytes)
                )
                free_gb = free_bytes.value / (1024**3)
            else:
                st = os.statvfs(os.getcwd())
                free_gb = (st.f_bavail * st.f_frsize) / (1024**3)
            
            if free_gb < required_space_gb:
                return False, f"Only {free_gb:.2f}GB free space available. {required_space_gb}GB required."
            return True, f"Sufficient disk space available: {free_gb:.2f}GB"
        except Exception as e:
            return False, f"Error checking disk space: {str(e)}"
    
    def check_ports(self, start_port: int = 5433, num_ports: int = 2) -> Tuple[bool, List[int]]:
        """Check if required ports are available"""
        available_ports = []
        for port in range(start_port, start_port + num_ports):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('localhost', port))
                    available_ports.append(port)
                except socket.error:
                    continue
        return len(available_ports) >= num_ports, available_ports
    
    def check_postgres_connection(self) -> bool:
        """Check if PostgreSQL is running and accessible"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                dbname=self.default_env_config['POSTGRES_DB'],
                user=self.default_env_config['POSTGRES_USER'],
                password=self.default_env_config['POSTGRES_PASSWORD'],
                host='localhost',
                port=5432
            )
            conn.close()
            return True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {str(e)}")
            print("\nPlease ensure PostgreSQL is running and accessible.")
            print("You can start PostgreSQL using:")
            if self.system == 'darwin':  # macOS
                print("brew services start postgresql")
            elif self.system == 'linux':
                print("sudo systemctl start postgresql")
            elif self.system == 'windows':
                print("net start postgresql")
            return False
    
    def run_subprocess(self, cmd, check=True, capture_output=False, shell=False):
        try:
            result = subprocess.run(cmd, check=check, capture_output=capture_output, text=True, shell=shell)
            return result
        except subprocess.CalledProcessError as e:
            if capture_output:
                print(e.stdout)
                print(e.stderr)
            return None

    def check_postgres_version(self) -> Tuple[bool, str]:
        try:
            result = self.run_subprocess(['psql', '--version'], capture_output=True)
            if result and result.returncode == 0:
                version_line = result.stdout.strip()
                match = re.search(r'(\d+\.\d+)', version_line)
                if match:
                    version = match.group(1)
                    major = version.split('.')[0]
                    return (major == self.db_version, version)
            return (False, '')
        except Exception:
            return (False, '')

    def install_postgres_16(self) -> bool:
        print(f"Installing PostgreSQL {self.db_version}...")
        if self.system == 'darwin':
            # macOS
            if shutil.which('brew'):
                self.run_subprocess(['brew', 'update'])
                self.run_subprocess(['brew', 'install', f'postgresql@{self.db_version}'])
                self.run_subprocess(['brew', 'services', 'start', f'postgresql@{self.db_version}'])
                print(f"PostgreSQL {self.db_version} installed and started via Homebrew.")
                return True
            else:
                print("Homebrew is required to install PostgreSQL on macOS. Please install Homebrew first: https://brew.sh")
                return False
        elif self.system == 'linux':
            # Linux
            if shutil.which('apt-get'):
                self.run_subprocess(['sudo', 'apt-get', 'update'])
                self.run_subprocess(['sudo', 'apt-get', 'install', '-y', 'wget', 'ca-certificates'])
                self.run_subprocess(['wget', '-qO-', 'https://www.postgresql.org/media/keys/ACCC4CF8.asc', '|', 'sudo', 'apt-key', 'add', '-'], shell=True)
                self.run_subprocess(['sudo', 'sh', '-c', f'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'], shell=True)
                self.run_subprocess(['sudo', 'apt-get', 'update'])
                self.run_subprocess(['sudo', 'apt-get', 'install', '-y', f'postgresql-{self.db_version}', f'postgresql-client-{self.db_version}'])
                self.run_subprocess(['sudo', 'systemctl', 'enable', f'postgresql'])
                self.run_subprocess(['sudo', 'systemctl', 'start', f'postgresql'])
                print(f"PostgreSQL {self.db_version} installed and started on Linux.")
                return True
            elif shutil.which('yum'):
                self.run_subprocess(['sudo', 'yum', 'install', '-y', 'https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %rhel)-x86_64/pgdg-redhat-repo-latest.noarch.rpm'], shell=True)
                self.run_subprocess(['sudo', 'yum', 'install', '-y', f'postgresql{self.db_version}-server', f'postgresql{self.db_version}'])
                self.run_subprocess(['sudo', f'/usr/pgsql-{self.db_version}/bin/postgresql-{self.db_version}-setup', 'initdb'])
                self.run_subprocess(['sudo', 'systemctl', 'enable', f'postgresql-{self.db_version}'])
                self.run_subprocess(['sudo', 'systemctl', 'start', f'postgresql-{self.db_version}'])
                print(f"PostgreSQL {self.db_version} installed and started on Linux.")
                return True
            else:
                print("Unsupported Linux distribution. Please install PostgreSQL 16 manually.")
                return False
        elif self.system == 'windows':
            print("Please download and install PostgreSQL 16 from https://www.enterprisedb.com/downloads/postgres-postgresql-downloads#windows")
            return False
        else:
            print("Unsupported OS for automated PostgreSQL installation.")
            return False

    def ensure_postgres_16(self) -> bool:
        has_pg, version = self.check_postgres_version()
        if has_pg:
            print(f"PostgreSQL {self.db_version} is already installed (version: {version})")
            self.start_postgres_service()
            return True
        else:
            print(f"PostgreSQL {self.db_version} is not installed or not the default version.")
            return self.install_postgres_16()

    def start_postgres_service(self):
        print("Ensuring PostgreSQL service is running...")
        if self.system == 'darwin':
            self.run_subprocess(['brew', 'services', 'start', f'postgresql@{self.db_version}'])
        elif self.system == 'linux':
            self.run_subprocess(['sudo', 'systemctl', 'start', 'postgresql'])
        # On Windows, user must start manually

    def create_omop_db(self):
        print(f"Ensuring database '{self.db_name}' exists...")
        try:
            result = self.run_subprocess(['psql', '-U', self.db_user, '-h', 'localhost', '-p', str(self.db_port), '-lqt'], capture_output=True)
            if result and self.db_name in result.stdout:
                print(f"Database '{self.db_name}' already exists.")
                return True
            # Create the database
            create_cmd = f"CREATE DATABASE {self.db_name};"
            self.run_subprocess(['psql', '-U', self.db_user, '-h', 'localhost', '-p', str(self.db_port), '-c', create_cmd])
            print(f"Database '{self.db_name}' created.")
            return True
        except Exception as e:
            print(f"Error creating database: {e}")
            print("You may need to ensure the 'postgres' user exists and has the correct password.")
            return False

    def create_postgres_user(self):
        print(f"Ensuring PostgreSQL user '{self.db_user}' exists...")
        try:
            # Check if the user exists
            result = self.run_subprocess(['psql', '-U', 'postgres', '-h', 'localhost', '-p', str(self.db_port), '-c', f"SELECT 1 FROM pg_roles WHERE rolname='{self.db_user}'"], capture_output=True)
            if result and '1 row' in result.stdout:
                print(f"User '{self.db_user}' already exists.")
                return True
            # Create the user
            create_user_cmd = f"CREATE USER {self.db_user} WITH PASSWORD '{self.db_password}';"
            self.run_subprocess(['psql', '-U', 'postgres', '-h', 'localhost', '-p', str(self.db_port), '-c', create_user_cmd])
            print(f"User '{self.db_user}' created.")
            # Grant privileges
            grant_cmd = f"GRANT ALL PRIVILEGES ON DATABASE {self.db_name} TO {self.db_user};"
            self.run_subprocess(['psql', '-U', 'postgres', '-h', 'localhost', '-p', str(self.db_port), '-c', grant_cmd])
            print(f"Privileges granted to '{self.db_user}'.")
            return True
        except Exception as e:
            print(f"Error creating user: {e}")
            return False

    def run_checks(self) -> bool:
        print("Running system checks...")
        if not self.check_python_version():
            return False
        if not self.create_virtual_environment():
            return False
        if not self.activate_virtual_environment():
            return False
        if not self.upgrade_pip():
            return False
        disk_ok, disk_msg = self.check_disk_space()
        print(f"Disk space check: {disk_msg}")
        if not disk_ok:
            return False
        docker_ok, docker_msg = self.check_docker()
        print(f"Docker check: {docker_msg}")
        if not docker_ok:
            print("Attempting to install Docker...")
            if not self.install_docker():
                return False
        postgres_ok, postgres_msg = self.check_postgres()
        print(f"PostgreSQL check: {postgres_msg}")
        if not postgres_ok:
            print("Attempting to install PostgreSQL client...")
            if not self.install_postgres():
                return False
        # Ensure PostgreSQL 16 is installed and running
        if not self.ensure_postgres_16():
            return False
        # Ensure omop_db exists
        if not self.create_omop_db():
            return False
        # Ensure postgres user exists and has privileges
        if not self.create_postgres_user():
            return False
        
        # Install Python packages first
        print("\nChecking and installing Python packages...")
        missing_packages = self.check_python_packages()
        if missing_packages:
            print(f"Installing missing packages: {missing_packages}")
            if not self.install_python_packages(missing_packages):
                return False
        
        # Verify all packages are installed
        missing_packages = self.check_python_packages()
        if missing_packages:
            print(f"Error: Failed to install packages: {missing_packages}")
            return False
        
        if not self.create_env_file():
            return False
        if not self.validate_env_config():
            print("\nPlease update the .env file with your actual configuration and run setup.py again.")
            return False
        print("\nAll checks passed! System is ready to run the partitioner.")
        return True

def main():
    checker = DependencyChecker()
    if checker.run_checks():
        print("\nYou can now run the partitioner:")
        print("1. Review and update the .env file with your actual database configuration if needed")
        print("2. The virtual environment is already activated")
        print("3. Run: python omop_partitioner.py")
    else:
        print("\nSome checks failed. Please fix the issues and run setup.py again.")

if __name__ == "__main__":
    main() 
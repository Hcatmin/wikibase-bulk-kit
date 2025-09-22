"""Configuration manager for loading and validating project settings."""

import yaml
from pathlib import Path
from typing import Any
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_login import Login

from .models import ProjectConfig


class ConfigManager:
    """Manages project configuration loading and validation."""
    
    def __init__(self, config_path: str) -> None:
        """Initialize configuration manager.
        
        Args:
            config_path: Path to the project configuration file
        """
        self.config_path = Path(config_path)
        self._config: ProjectConfig | None = None
        self._wbi: WikibaseIntegrator | None = None
    
    def load_config(self) -> ProjectConfig:
        """Load and validate project configuration.
        
        Returns:
            Validated project configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
            ValidationError: If config doesn't match schema
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        self._config = ProjectConfig(**config_data)
        return self._config
    
    @property
    def config(self) -> ProjectConfig:
        """Get the loaded configuration.
        
        Returns:
            Project configuration (loads if not already loaded)
        """
        if self._config is None:
            self._config = self.load_config()
        return self._config
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a setting from the configuration.
        
        Args:
            key: Setting key (supports dot notation)
            default: Default value if key not found
            
        Returns:
            Setting value or default
        """
        config = self.config
        
        # Handle dot notation for nested settings
        keys = key.split('.')
        value = config.settings
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_wikibase_url(self) -> str:
        """Get Wikibase instance URL."""
        return self.config.wikibase.url
    
    def get_data_dir(self) -> Path:
        """Get data directory path."""
        return Path(self.config.data_dir)
    
    def get_wikibase_integrator(self) -> WikibaseIntegrator:
        """Get configured Wikibase Integrator instance.
        
        Returns:
            Configured WikibaseIntegrator instance
        """
        if self._wbi is None:
            # Configure Wikibase Integrator
            config = self.config
            wbi_config['MEDIAWIKI_API_URL'] = f"{config.wikibase.url}/w/api.php"
            wbi_config['WIKIBASE_URL'] = config.wikibase.url
            wbi_config['SPARQL_ENDPOINT_URL'] = config.wikibase.sparql_endpoint
            
            # Initialize Wikibase Integrator
            self._wbi = WikibaseIntegrator()
            
            # Set up authentication if credentials are provided
            if config.wikibase.username and config.wikibase.password:
                login = Login(
                    user=config.wikibase.username,
                    password=config.wikibase.password
                )
                self._wbi.login = login
            elif config.wikibase.token:
                # For token-based authentication
                login = Login(token=config.wikibase.token)
                self._wbi.login = login
        
        return self._wbi

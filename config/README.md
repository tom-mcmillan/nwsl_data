# Configuration Management

This directory contains environment-specific configuration files for the NWSL Analytics platform.

## Configuration Files

### YAML Configurations (Recommended)
- **`development.yml`**: Development environment with debug features enabled
- **`production.yml`**: Production environment optimized for performance and reliability
- **`testing.yml`**: Testing environment with minimal resources and fast execution

### Legacy Configuration
- **`server_config.json`**: Legacy JSON configuration (maintained for compatibility)

## Usage

### Environment Selection
Set the `NWSL_ENV` environment variable to choose configuration:

```bash
# Development (default)
export NWSL_ENV=development

# Production
export NWSL_ENV=production

# Testing
export NWSL_ENV=testing
```

### Configuration Loading
The application loads configuration in this order:
1. Environment-specific YAML file (`{NWSL_ENV}.yml`)
2. Environment variables (override YAML values)
3. Fallback to legacy `server_config.json`

### Environment Variable Overrides
Any configuration value can be overridden with environment variables using dot notation:

```bash
export NWSL_SERVER__PORT=9000
export NWSL_DATABASE__PATH="/custom/path/to/db"
export NWSL_ANALYTICS__NIR_CALCULATION__ENABLE_CACHING=false
```

## Configuration Sections

### Server
- Basic server settings (port, host, debug mode)
- Production vs development optimizations

### Database
- Database connection parameters
- Connection pooling and timeouts

### Analytics
- NIR calculation settings
- Visualization preferences
- Caching configuration

### Logging
- Log levels and formatting
- Output destinations

### Tools
- MCP tool limits and features
- Experimental feature flags

## Security Notes

- Never commit sensitive values (API keys, passwords) to these files
- Use environment variables for sensitive configuration
- Review production settings before deployment
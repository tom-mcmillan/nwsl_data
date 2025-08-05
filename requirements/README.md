# Requirements Management

This directory contains separated dependency files for different environments:

## Files

- **`base.txt`**: Core dependencies required for all environments
- **`visualization.txt`**: Adds visualization and chart generation dependencies
- **`dev.txt`**: Development dependencies including testing and code quality tools
- **`prod.txt`**: Production dependencies optimized for deployment

## Usage

### Development Environment
```bash
pip install -r requirements/dev.txt
```

### Production Environment
```bash
pip install -r requirements/prod.txt
```

### Visualization Only
```bash
pip install -r requirements/visualization.txt
```

### Base Installation
```bash
pip install -r requirements/base.txt
```

## Dependency Chain

```
base.txt (core)
    ↓
visualization.txt (adds charts)
    ↓
dev.txt (adds development tools)
prod.txt (production optimized)
```
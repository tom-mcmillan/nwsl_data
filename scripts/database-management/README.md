# Database Management Scripts

Scripts for database schema management and data maintenance.

## Schema Management
- `add_2013_team_data.py` - Add historical 2013 team data
- `create_team_venue_region_table.py` - Create venue/region reference tables

## Data Import
- `import_city_data.py` - Import city/location reference data
- `update_venue_addresses.py` - Update venue address information

## Usage

### Initial Setup
```bash
python database-management/create_team_venue_region_table.py
python database-management/import_city_data.py
```

### Data Updates
```bash
python database-management/update_venue_addresses.py
python database-management/add_2013_team_data.py
```

## Dependencies
- sqlite3
- pandas
- Database schema definitions
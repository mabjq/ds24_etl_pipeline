import shutil
import datetime
import os

# Paths
db_path = "data/prices.db"
backup_path = f"data/prices_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"

# Create backup directory if it doesn't exist
os.makedirs(os.path.dirname(backup_path), exist_ok=True)

# Copy DB to backup
shutil.copy2(db_path, backup_path)
print(f"Backup created at {backup_path}")
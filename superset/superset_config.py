import os

# Configure PostgreSQL as metadata database
SQLALCHEMY_DATABASE_URI = "postgresql://postgres:postgres@postgres:5432/postgres"

# Set Superset home directory for persistence
DATA_DIR = "/app/superset_home"

# Other configurations
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "this_should_be_changed")

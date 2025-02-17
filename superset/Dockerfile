FROM apache/superset:latest

# Switch to root to install required dependencies
USER root

# Install Dremio, Trino, and S3-related packages
RUN pip install trino s3fs psycopg2-binary

# Set up environment variables
ENV SUPERSET_SECRET_KEY=XgiVZQdtlIIeqUmo8Rs2d5hjuQZrXsJh

# Set ownership of superset_home directory for persistence
RUN mkdir -p /app/superset_home && chown -R superset:superset /app/superset_home

# Switch back to the Superset user
USER superset

# Copy the custom Superset configuration
COPY superset_config.py /etc/superset/

# Define entrypoint script
COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

# Set working directory
WORKDIR /app

# Set default command
CMD ["/entrypoint.sh"]

# Use a specific version of the Python image
FROM python:3.12-slim

# Install psycopg2-binary dependency
RUN pip install psycopg2-binary

# Set the working directory inside the container to /opt/datagen
WORKDIR /opt/datagen

# Copy the files from your datagen folder into the container
COPY ../datagen /opt/datagen/

# Make sure data_gen.py is executed when the container starts
CMD ["python", "./data_gen.py"]
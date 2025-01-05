# docker/Dockerfile

# 1) Start from a lightweight Python image
FROM python:3.10-slim

# 2) Set a working directory inside the container
WORKDIR /app

# 3) Copy the requirements file from your config folder into the container
COPY ../config/requirements.txt ./requirements.txt

# 4) Install packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 5) Copy the source code (the entire src/ directory) into the container
COPY ../src/ ./src/

# Optional: If you have a web server or anything that needs a port, EXPOSE it.
# EXPOSE 5000

# 6) Set a default command. 
#    We'll assume you want to run mta_data_collector.py by default.
CMD ["python", "src/mta_data_collector.py"]

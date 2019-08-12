FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD export_reach_contact_list.py /app

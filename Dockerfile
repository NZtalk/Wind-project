# Dockerfile for the Python service
FROM python:3.9-slim

ARG POETRY_HOME=/opt/poetry
ENV PATH="${POETRY_HOME}/bin:${PATH}"

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    curl \
    gcc \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy the project files
COPY pyproject.toml poetry.lock ./
COPY src/ ./src/

# Install project dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

# Set the entrypoint and command for the container
ENTRYPOINT []

# Start the Python application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]




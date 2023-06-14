FROM python:3.11-slim

ARG POETRY_HOME=/etc/poetry
ENV PATH="${PATH}:${POETRY_HOME}/bin"

# set the working directory
WORKDIR /app
COPY poetry.lock pyproject.toml ./

# install dependencies
RUN apt update -y && apt upgrade -y && apt install curl -y && apt install gcc -y
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=${POETRY_HOME} python3 -
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev 

# copy the src to the folder
COPY .env .
COPY ./src ./src

# start the server
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]
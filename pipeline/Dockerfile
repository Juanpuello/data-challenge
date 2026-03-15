FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked --no-dev

COPY main.py ./
COPY src/ ./src/
COPY assets/ ./assets/

ENTRYPOINT ["uv", "run", "python", "main.py"]

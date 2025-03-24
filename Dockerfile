FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy requirements
COPY requirements.txt .

# Install dependencies using uv with --system flag
RUN uv pip install --system -r requirements.txt

# Copy source code
COPY . .

# Set Python path
ENV PYTHONPATH=/app

CMD ["uv", "run", "produce.py"] 
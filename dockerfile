FROM python:3.11.9-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /ws_batch_transform

RUN pip install uv

# Copy the application to the image
COPY . .

# Install dependencies using UV
RUN uv pip install --system -r requirements.txt

# Run your script
CMD ["python", "src/main.py"]
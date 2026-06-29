# Apify's Python + Playwright base image ships Chromium and all its system deps.
FROM apify/actor-python-playwright:3.12

# Install Python dependencies first to leverage Docker layer caching.
COPY requirements.txt ./
RUN echo "Python version:" \
    && python --version \
    && echo "Installing dependencies:" \
    && pip install --no-cache-dir -r requirements.txt \
    && echo "All dependencies installed."

COPY . ./

CMD ["python", "-m", "src.main"]

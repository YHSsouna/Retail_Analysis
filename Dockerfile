FROM apache/airflow:2.10.5-python3.9

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    software-properties-common \
    libnss3 \
    libgbm-dev \
    libgconf-2-4 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft Edge (Stable)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && add-apt-repository "deb [arch=amd64] https://packages.microsoft.com/repos/edge stable main" \
    && apt-get update \
    && apt-get install -y microsoft-edge-stable \
    && rm -rf /var/lib/apt/lists/*

# Install Edge WebDriver (match installed Edge version)
RUN EDGE_VERSION=$(microsoft-edge --version | awk '{print $NF}') \
    && wget -O /tmp/edgedriver.zip "https://msedgedriver.azureedge.net/${EDGE_VERSION}/edgedriver_linux64.zip" \
    && unzip /tmp/edgedriver.zip -d /usr/local/bin/ \
    && rm /tmp/edgedriver.zip

# Switch to Airflow user
USER airflow

# Install dbt-core and dbt-postgres
RUN pip install --no-cache-dir dbt-core==1.5.1 dbt-postgres==1.5.1
# Copy and Install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /opt/airflow/requirements.txt




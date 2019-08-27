FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Install Chrome (for generating PNGs of graphs)
ARG CHROME_VERSION="google-chrome-stable"
RUN apt-get update && apt-get install -y wget gnupg \
  && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update -qqy \
  && apt-get -qqy install \
    ${CHROME_VERSION:-google-chrome-stable} \
  && rm /etc/apt/sources.list.d/google-chrome.list \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Install Chrome web driver (for connecting to Chrome from Python)
ARG CHROME_DRIVER_VERSION
RUN if [ -z "$CHROME_DRIVER_VERSION" ]; \
  then CHROME_MAJOR_VERSION=$(google-chrome --version | sed -E "s/.* ([0-9]+)(\.[0-9]+){3}.*/\1/") \
    && CHROME_DRIVER_VERSION=$(wget --no-verbose -O - "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAJOR_VERSION}"); \
  fi \
  && apt-get update && apt-get install unzip \
  && echo "Using chromedriver version: "$CHROME_DRIVER_VERSION \
  && wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip \
  && rm -rf /opt/selenium/chromedriver \
  && unzip /tmp/chromedriver_linux64.zip -d /opt/selenium \
  && rm /tmp/chromedriver_linux64.zip \
  && mv /opt/selenium/chromedriver /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && chmod 755 /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && ln -fs /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION /usr/bin/chromedriver

# Install pyflame (for statistical CPU profiling) if this script is run with PROFILE_CPU flag
ARG INSTALL_CPU_PROFILER="false"
RUN if [ "$INSTALL_CPU_PROFILER" = "true" ]; then \
        apt-get update && apt-get install -y autoconf automake autotools-dev g++ pkg-config python-dev python3-dev libtool make && \
        git clone https://github.com/uber/pyflame.git /pyflame && cd /pyflame && git checkout "v1.6.7" && \
        ./autogen.sh && ./configure && make && make install && \
        rm -rf /pyflame; \
    fi

# Install memory_profiler if this script is run with PROFILE_MEMORY flag
ARG INSTALL_MEMORY_PROFILER="false"
RUN if [ "$INSTALL_MEMORY_PROFILER" = "true" ]; then \
        apt-get update && apt-get install -y gcc && \
        pip install memory_profiler; \
    fi

# Make a directory for private credentials files
RUN mkdir /credentials

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD code_schemes/*.json /app/code_schemes/
ADD src /app/src
ADD fetch_raw_data.py /app
#ADD fetch_flow_definitions.py /app
ADD generate_outputs.py /app
ADD upload_logs.py /app
ADD generate_analysis_graphs.py /app

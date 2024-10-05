FROM continuumio/miniconda3

COPY dev-env.yml /tmp/dev-env.yml

RUN conda env create -f /tmp/dev-env.yml

SHELL ["conda", "run", "-n", "dev-env", "/bin/bash", "-c"]

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    lsb-release \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && apt-get install -y unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]
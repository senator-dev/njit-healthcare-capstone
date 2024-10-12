FROM continuumio/miniconda3

COPY dev-env.yml /tmp/dev-env.yml

RUN conda env create -f /tmp/dev-env.yml

SHELL ["conda", "run", "-n", "dev-env", "/bin/bash", "-c"]


RUN apt-get update && apt-get install -y \
    chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV DISPLAY=:99
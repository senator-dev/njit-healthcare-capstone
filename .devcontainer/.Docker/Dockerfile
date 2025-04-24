FROM pytorch/pytorch:latest

COPY conda_requirements.txt /tmp/conda_requirements.txt

COPY pip_requirements.txt /tmp/pip_requirements.txt

RUN conda install --yes --file /tmp/conda_requirements.txt

RUN pip install -r /tmp/pip_requirements.txt

RUN apt update

RUN apt install git -y
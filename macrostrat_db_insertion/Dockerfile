FROM ubuntu:22.04
RUN apt update -y && apt upgrade -y

RUN apt install -y g++ \
         wget \
         git \
         curl \
         libpq-dev

RUN wget \
   https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
   && bash Miniconda3-latest-Linux-x86_64.sh -b -p /conda \
   && rm -f Miniconda3-latest-Linux-x86_64.sh

ENV PATH=${PATH}:/conda/bin
# Enables "source activate conda"
SHELL ["/bin/bash", "-c"]
RUN conda init
RUN source ~/.bashrc

WORKDIR /app

# Create and activate conda environment
COPY environment.yml environment.yml
RUN conda env create -f environment.yml
SHELL ["conda", "run", "-n", "db_insert_env", "/bin/bash", "-c"]

COPY . ./macrostrat_db_insertion

# Add the macrostrat_db_insertion directory to PYTHONPATH
ENV PYTHONPATH=.

# The code to run when the container is started
EXPOSE 9543
ENTRYPOINT ["conda", "run", "-n", "db_insert_env", "python3", "macrostrat_db_insertion/server.py"]
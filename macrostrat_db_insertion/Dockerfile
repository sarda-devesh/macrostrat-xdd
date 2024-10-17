FROM ubuntu:22.04
RUN apt-get update && apt update -y && apt upgrade -y

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

RUN mkdir /working_dir
COPY . /working_dir
WORKDIR /working_dir

# Create and activate conda environment
RUN conda env create -f environment.yml
SHELL ["conda", "run", "-n", "db_insert_env", "/bin/bash", "-c"]

# The code to run when the container is started
EXPOSE 9543
ENTRYPOINT ["conda", "run", "-n", "db_insert_env", "python3", "server.py"]
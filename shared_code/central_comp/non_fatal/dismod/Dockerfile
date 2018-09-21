FROM ubuntu
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    git \
    htop \
    vim \
    wget \
    gsl-bin \
    gsl-bin \
    libgsl0-dev \
    libblas-dev \
    liblapack-dev \
&& rm -rf /var/lib/apt/lists/*

# INSTALL MINICONDA
RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
RUN bash Miniconda2-latest-Linux-x86_64.sh -p /miniconda -b
RUN rm Miniconda2-latest-Linux-x86_64.sh
ENV PATH=/miniconda/bin:${PATH}
RUN conda update -y conda
RUN conda install -y numpy
RUN pip install matplotlib

# INSTALL DISMOD
RUN wget http://moby.ihme.washington.edu/USER/dismod_ode/dismod_ode-20160714.tgz
RUN tar -xvf dismod_ode-20160714.tgz && mv dismod_ode-20160714 dismod_ode

COPY dismod_patches/setup.sh /dismod_ode/bin/setup.sh
COPY dismod_patches/data_model.py.in /dismod_ode/python/data_model.py.in
WORKDIR /dismod_ode
RUN bin/setup.sh
WORKDIR build
RUN make check
RUN make install

# INSTALL REQUIREMENTS
COPY pip.conf /etc/pip.conf
COPY conda_requirements.txt /tmp/conda_requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN conda install --file /tmp/conda_requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p strDir
WORKDIR strDir
VOLUME strDir strDir

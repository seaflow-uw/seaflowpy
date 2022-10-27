ARG ARG_PYTHON_VERSION=3.9
ARG ARG_BASE_IMAGE=${ARG_PYTHON_VERSION}-slim-buster

FROM python:${ARG_BASE_IMAGE} AS build_image
ARG ARG_PYTHON_VERSION
ENV PYTHONUNBUFFERED=1

RUN apt-get update -qq \
    && apt-get install -qq -y build-essential git sqlite3 \
    && rm -rf /var/lib/apt/lists/*
RUN adduser --quiet --disabled-password --gecos '' seaflow

WORKDIR /seaflowpy/

# deps into their own layer
COPY requirements-test.txt /seaflowpy/requirements-test.txt
RUN pip3 install -r ./requirements-test.txt

COPY . /seaflowpy/
RUN git clean -qfdx \
    && python3 setup.py sdist bdist_wheel \
    && pip3 install --no-index --no-deps dist/*.whl \
    && rm -rf __pycache__ build/

CMD ["bash"]

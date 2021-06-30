ARG ARG_PYTHON_VERSION=3.8
ARG ARG_BASE_IMAGE=${ARG_PYTHON_VERSION}-slim-buster

FROM python:${ARG_BASE_IMAGE} AS build_image
ARG ARG_PYTHON_VERSION
ENV PYTHONUNBUFFERED=1
ENV PATH=/home/seaflow/.local/bin:$PATH

RUN apt-get update -qq \
    && apt-get install -qq -y build-essential git sqlite3 \
    && rm -rf /var/lib/apt/lists/*
RUN adduser --quiet --disabled-password --gecos '' seaflow

USER seaflow
WORKDIR /seaflowpy/

# deps into their own layer
COPY --chown=seaflow:seaflow requirements-test.txt /seaflowpy/requirements-test.txt
RUN pip3 install --user -r ./requirements-test.txt

COPY --chown=seaflow:seaflow . /seaflowpy/
RUN git clean -qfdx \
    && python3 setup.py sdist bdist_wheel \
    && pip3 install --user --no-index --no-deps dist/*.whl \
    && rm -rf __pycache__ build/

CMD ["bash"]

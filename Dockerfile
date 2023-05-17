ARG ARG_PYTHON_VERSION=3.11

FROM python:${ARG_PYTHON_VERSION}-slim-bullseye
ENV PYTHONUNBUFFERED=1

RUN apt-get update -qq \
    && apt-get install -qq -y git sqlite3 \
    && rm -rf /var/lib/apt/lists/*
RUN adduser --quiet --disabled-password --gecos '' seaflow

WORKDIR /seaflowpy/

# deps into their own layer
COPY requirements-test.txt /seaflowpy/requirements-test.txt
RUN pip3 install --compile -r ./requirements-test.txt

COPY . /seaflowpy/
RUN git clean -qfdx \
    && python3 setup.py sdist bdist_wheel \
    && pip3 install --compile --no-index --no-deps dist/*.whl \
    && pip3 cache purge \
    && rm -rf build/

CMD ["bash"]

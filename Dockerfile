ARG ARG_PYTHON_VERSION=3.13

FROM python:${ARG_PYTHON_VERSION}-slim-bookworm

RUN apt-get update -qq \
    && apt-get install -qq -y git sqlite3 zst \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --quiet --disabled-password --gecos '' seaflow

ENV VIRTUAL_ENV=/venv \
    PYTHONUNBUFFERED=1
RUN python -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY build-requirements.txt requirements.txt
RUN pip install --no-cache-dir --compile -r requirements.txt

ARG ARG_APP_VERSION
COPY dist/seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl dist/seaflowpy-${ARG_APP_VERSION}.tar.gz ./
RUN pip install --disable-pip-version-check --no-cache-dir --compile seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl \
    && mkdir seaflowpy-src \
    && tar -C seaflowpy-src -zxf seaflowpy-${ARG_APP_VERSION}.tar.gz \
    && rm seaflowpy-${ARG_APP_VERSION}.tar.gz

CMD ["bash"]

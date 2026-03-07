ARG ARG_PYTHON_VERSION=3.13

FROM python:${ARG_PYTHON_VERSION}-slim-bookworm

ARG ARG_APP_VERSION
RUN test -n "${ARG_APP_VERSION}" || { echo "ERROR: --build-arg ARG_APP_VERSION=<version> is required"; exit 1; }

ARG ARG_BUILD_REQUIREMENTS
RUN test -n "${ARG_BUILD_REQUIREMENTS}" || { echo "ERROR: --build-arg ARG_BUILD_REQUIREMENTS=<requirements.txt> is required"; exit 1; }

RUN apt-get update -qq \
    && apt-get install -qq -y git sqlite3 zst \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --quiet --disabled-password --gecos '' seaflow

ENV VIRTUAL_ENV=/venv \
    PYTHONUNBUFFERED=1
RUN python -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY ${ARG_BUILD_REQUIREMENTS} requirements.txt
RUN pip install --no-cache-dir --compile -r requirements.txt

COPY dist/seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl dist/seaflowpy-${ARG_APP_VERSION}.tar.gz ./
RUN pip install --disable-pip-version-check --no-cache-dir --compile seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl \
    && mkdir seaflowpy-src \
    && tar -C seaflowpy-src -zxf seaflowpy-${ARG_APP_VERSION}.tar.gz \
    && rm seaflowpy-${ARG_APP_VERSION}.tar.gz

CMD ["bash"]

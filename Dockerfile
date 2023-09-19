ARG ARG_PYTHON_VERSION=3.11
ARG ARG_BUILDER_IMAGE=${ARG_PYTHON_VERSION}-bookworm
ARG ARG_RUNTIME_IMAGE=${ARG_PYTHON_VERSION}-slim-bookworm

FROM python:${ARG_BUILDER_IMAGE} as builder

RUN apt-get update -qq \
    && apt-get install -qq -y build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/venv \
    PYTHONUNBUFFERED=1
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN python -m venv /venv

COPY build-requirements.txt .
RUN pip install --no-cache-dir --compile -r build-requirements.txt

FROM python:${ARG_RUNTIME_IMAGE} as runtime

ENV VIRTUAL_ENV=/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN apt-get update -qq \
    && apt-get install -qq -y git \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --quiet --disabled-password --gecos '' seaflow

COPY --from=builder build-requirements.txt requirements.txt
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
ARG ARG_APP_VERSION
COPY dist/seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl dist/seaflowpy-${ARG_APP_VERSION}.tar.gz ./
RUN pip install --disable-pip-version-check --no-cache-dir --compile seaflowpy-${ARG_APP_VERSION}-py3-none-any.whl \
    && mkdir seaflowpy-src \
    && tar -C seaflowpy-src -zxf seaflowpy-${ARG_APP_VERSION}.tar.gz \
    && rm seaflowpy-${ARG_APP_VERSION}.tar.gz

CMD ["bash"]

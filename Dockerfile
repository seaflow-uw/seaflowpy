ARG ARG_PYTHON_VERSION=3.8
ARG ARG_BASE_IMAGE=${ARG_PYTHON_VERSION}-slim-buster
ARG ARG_INSTALL_PREFIX=/seaflowpy

FROM python:${ARG_BASE_IMAGE} AS build_image
ARG ARG_PYTHON_VERSION
ARG ARG_INSTALL_PREFIX
ENV PYTHONUNBUFFERED=1 \
    PATH=${ARG_INSTALL_PREFIX}/bin:${PATH} \
    PYTHONPATH=${ARG_INSTALL_PREFIX}/lib/python${ARG_PYTHON_VERSION}/site-packages

RUN apt-get update -qq \
    && apt-get install -qq -y build-essential git \
    && rm -rf /var/lib/apt/lists/*
COPY  . /seaflowpy-src/
WORKDIR /seaflowpy-src
RUN git clean -qfdx \
    && python setup.py sdist bdist_wheel \
    && pip3 install --prefix ${ARG_INSTALL_PREFIX} --no-cache-dir -r ./requirements-test.txt \
    && pip3 install --prefix ${ARG_INSTALL_PREFIX} --no-cache-dir --no-index --no-deps . \
    && mkdir ${ARG_INSTALL_PREFIX}-dist \
    && cp ./dist/*.tar.gz ./dist/*.whl ${ARG_INSTALL_PREFIX}-dist/ \
    && mkdir ${ARG_INSTALL_PREFIX}-tests/ \
    && cp -r ./tests ${ARG_INSTALL_PREFIX}-tests/ \
    && cp -r ./pytest.ini ${ARG_INSTALL_PREFIX}-tests/


FROM python:${ARG_BASE_IMAGE} AS runtime_image
ARG ARG_PYTHON_VERSION
ARG ARG_INSTALL_PREFIX
ENV PYTHONUNBUFFERED=1 \
    PATH=${ARG_INSTALL_PREFIX}/bin:${PATH} \
    PYTHONPATH=${ARG_INSTALL_PREFIX}/lib/python${ARG_PYTHON_VERSION}/site-packages

RUN adduser --quiet --disabled-password --gecos '' seaflow
COPY --from=build_image --chown=seaflow:seaflow ${ARG_INSTALL_PREFIX} ${ARG_INSTALL_PREFIX}/
COPY --from=build_image --chown=seaflow:seaflow ${ARG_INSTALL_PREFIX}-tests ${ARG_INSTALL_PREFIX}-tests/
COPY --from=build_image --chown=seaflow:seaflow ${ARG_INSTALL_PREFIX}-dist ${ARG_INSTALL_PREFIX}-dist/
USER seaflow

CMD ["bash"]

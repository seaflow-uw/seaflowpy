ARG ARG_PYTHON_VERSION=3.11

FROM python:${ARG_PYTHON_VERSION}-slim-bookworm as build
ENV PYTHONUNBUFFERED=1

RUN apt-get update -qq \
    && apt-get install -qq -y git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /seaflowpy/

COPY .git .git/
ARG ARG_GIT_REF_COMMIT
RUN git clean -qfdx \
    && git checkout ${ARG_GIT_REF_COMMIT} -b buildbranch \
    && git reset --hard \
    && git status >git-info-for-this-install.txt \
    && git log -n 1 >>git-info-for-this-install.txt
RUN python3 -m venv /venv/ \
    && /venv/bin/pip3 install -U pip wheel setuptools \
    && /venv/bin/pip3 install --compile -r ./requirements-test.txt \
    && /venv/bin/python3 setup.py sdist bdist_wheel \
    && /venv/bin/pip3 install --compile --no-index --no-deps dist/*.whl \
    && rm -rf .git build


FROM python:${ARG_PYTHON_VERSION}-slim-bookworm as runtime
ENV PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/venv \
    PATH="/venv/bin:$PATH"

RUN apt-get update -qq \
    && apt-get install -qq -y sqlite3 git procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /seaflowpy/
COPY --from=build /venv /venv/
COPY --from=build /seaflowpy/ ./
RUN adduser --quiet --disabled-password --gecos '' seaflow
CMD ["bash"]

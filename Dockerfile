ARG PYTHON_VERSION=3.7-slim-stretch

# Build step
FROM python:${PYTHON_VERSION} as builder
ENV PYTHONUNBUFFERED 1

RUN mkdir /seaflowpy /wheels

COPY setup.py setup.cfg versioneer.py LICENSE README.md MANIFEST.in /seaflowpy/
COPY src/ /seaflowpy/src/
COPY .git /seaflowpy/.git
COPY requirements.txt /wheels/

WORKDIR /wheels

RUN apt-get update && apt-get install -y git \
    && pip3 wheel -r requirements.txt \
    && pip3 wheel --no-deps /seaflowpy

# Create final image
FROM python:${PYTHON_VERSION}
ENV PYTHONUNBUFFERED 1
COPY --from=builder /wheels /wheels
RUN pip3 install --no-cache-dir -r /wheels/requirements.txt -f /wheels/ \
    && pip3 install --no-cache-dir -f /wheels/ seaflowpy \
    && rm -rf /wheels

CMD ["bash"]

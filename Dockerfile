ARG PYTHON_VERSION=3.7-slim-stretch

FROM python:${PYTHON_VERSION} AS build_image
ENV PYTHONUNBUFFERED 1
ENV PATH=/usr/local/seaflowpy/bin:$PATH
ENV PYTHONPATH /usr/local/seaflowpy/lib/python3.7/site-packages/

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /wheels

COPY dist/*.whl requirements.txt /wheels/

# Install with --prefix to keep everything in one place, make it easier to
# copy to the final image.
RUN pip3 install --prefix /usr/local/seaflowpy --no-cache-dir -r /wheels/requirements.txt \
    && pip3 install --prefix /usr/local/seaflowpy --no-cache-dir --no-index -f /wheels seaflowpy \
    && rm -rf /wheels

FROM python:${PYTHON_VERSION} AS runtime_image

COPY --from=build_image /usr/local/seaflowpy /usr/local/seaflowpy
ENV PATH=/usr/local/seaflowpy/bin:$PATH
ENV PYTHONPATH /usr/local/seaflowpy/lib/python3.7/site-packages/

CMD ["bash"]

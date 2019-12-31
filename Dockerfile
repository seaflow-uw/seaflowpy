ARG PYTHON_VERSION=3.7-slim-stretch

FROM python:${PYTHON_VERSION} AS build_image
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /wheels

COPY dist/*.whl requirements.txt /wheels/

# Install with --user to keep everything in /root/.local, make it easier to
# copy to the final image.
RUN pip3 install --user --no-cache-dir -r /wheels/requirements.txt \
    && pip3 install --user --no-cache-dir --no-index -f /wheels seaflowpy \
    && rm -rf /wheels

FROM python:${PYTHON_VERSION} AS runtime_image

COPY --from=build_image /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

CMD ["bash"]

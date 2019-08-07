ARG PYTHON_VERSION=3.7-slim-stretch

FROM python:${PYTHON_VERSION}
ENV PYTHONUNBUFFERED 1

RUN mkdir /wheels

COPY dist/*.whl requirements.txt /wheels/

RUN pip3 install --no-cache-dir -r /wheels/requirements.txt \
    && pip3 install --no-cache-dir --no-index -f /wheels seaflowpy \
    && rm -rf /wheels

CMD ["bash"]

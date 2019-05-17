FROM python:3.7-slim-stretch

RUN mkdir /seaflowpy

COPY setup.py setup.cfg LICENSE README.md MANIFEST.in requirements.txt /seaflowpy/
COPY src/ /seaflowpy/src/

RUN pip3 install --no-cache-dir -r /seaflowpy/requirements.txt \
    && pip3 install --no-cache-dir /seaflowpy

CMD ["bash"]

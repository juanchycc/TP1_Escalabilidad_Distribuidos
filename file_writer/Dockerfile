FROM python:3.9.7-slim 
COPY . /
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/bin/sh"]
# TODO: Check image being used/requirements.txt
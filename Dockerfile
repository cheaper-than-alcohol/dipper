FROM python

RUN useradd -ms /bin/bash python

COPY *.py requirements.txt settings.json /home/python/
RUN mkdir -p /home/python/log; chown python:python /home/python/*; pip install -r /home/python/requirements.txt

ARG db_user
ARG db_pass
ARG db_address
ENV DB_USER=${db_user}
ENV DB_PASS=${db_pass}
ENV DB_ADDRESS=${db_address}

USER python
CMD cd ~; python pull_observations.py
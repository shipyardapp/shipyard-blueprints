FROM public.ecr.aws/lambda/python:3.9
CMD [ "main.handler" ]

ENV POETRY_VIRTUALENVS_IN_PROJECT=true

COPY ./src/shipyard-blueprints/ ${LAMBDA_TASK_ROOT}/shipyard-blueprints/
RUN chmod --recursive 0755 ${LAMBDA_TASK_ROOT}/shipyard-blueprints/

RUN pip install --upgrade pip poetry

WORKDIR ${LAMBDA_TASK_ROOT}/shipyard-blueprints/
RUN poetry install
RUN poetry build
RUN poetry env info

ENV SHIPYARD__CREDENTIALTESTING_REPODIR=${LAMBDA_TASK_ROOT}/shipyard-blueprints/
# This path depends on POETRY_VIRTUALENVS_IN_PROJECT above.
# The name of the executable matters. Ie. making a symlink to this file does not
# invoke the virtual environment correctly.
ENV SHIPYARD__CREDENTIALTESTING_PYTHON=${SHIPYARD__CREDENTIALTESTING_REPODIR}/.venv/bin/python
# Ensure SHIPYARD__CREDENTIALTESTING_PYTHON is executable.
RUN [ "/bin/bash", "-c", "[[ -x ${SHIPYARD__CREDENTIALTESTING_PYTHON} ]]" ]

COPY ./src/python/credentialtesting/main.py ${LAMBDA_TASK_ROOT}/main.py
WORKDIR ${LAMBDA_TASK_ROOT}

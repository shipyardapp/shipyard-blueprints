#FROM public.ecr.aws/lambda/python:3.9
FROM debian:11
CMD [ "main.handler" ]

## Extras Add LAMBDA_TASK_ROOT for parity
ENV LAMBDA_TASK_ROOT=/var/task/
RUN mkdir -p ${LAMBDA_TASK_ROOT}
RUN apt update -yq
RUN apt install -yq python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN which python
RUN python --version
## End of Extras to get to parity

ARG VENDOR
ARG EXTRA_PACKAGES=""
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install --upgrade pip poetry

COPY . /app

WORKDIR app/shipyard_blueprints/${VENDOR}
RUN poetry install

# Add Poetry's virtual environment to PATH
ENV PATH="/app/shipyard_blueprints/${VENDOR}/.venv/bin:$PATH"

RUN if [ -n "${EXTRA_PACKAGES}" ]; then \
    pip install ${EXTRA_PACKAGES}; \
    fi

# ENTRYPOINT ["python"]

FROM public.ecr.aws/lambda/python:3.9

ARG VENDOR
ARG EXTRA_PACKAGES=""
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install --upgrade pip poetry

COPY . app/

WORKDIR app/shipyard_blueprints/${VENDOR}
RUN poetry install

RUN if [ -n "${EXTRA_PACKAGES}" ]; then \
    pip install ${EXTRA_PACKAGES}; \
    fi


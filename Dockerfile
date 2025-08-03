FROM python:latest

WORKDIR /app

RUN apt-get update \
   && apt-get install -y \
   git \
   texlive-xetex \
   texlive-fonts-recommended \
   texlive-plain-generic \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --with dev

RUN poetry run jupyter notebook --generate-config \
   && echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py \
   && echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py \
   && echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py \
   && echo "c.NotebookApp.password_required = False" >> /root/.jupyter/jupyter_notebook_config.py

COPY . .

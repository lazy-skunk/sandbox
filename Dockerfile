FROM python:3.12

WORKDIR /app

RUN apt-get update \
 && apt-get install -y \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --upgrade pip \
 && pip install .[dev]

RUN jupyter notebook --generate-config \
 && echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py \
 && echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py

COPY . .

ENTRYPOINT ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
FROM waggle/plugin-base:1.1.1-base

# install requirements
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --upgrade -r /app/requirements.txt

COPY app /app/
WORKDIR /app
ENTRYPOINT ["python3", "/app/app.py"]

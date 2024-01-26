# syntax=docker/dockerfile:1

# start by pulling the python image
FROM python:3.10

# switch working directory
WORKDIR /finalProjectAPI

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copy every content from the local file to the image
COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
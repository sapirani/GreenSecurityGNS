FROM hadoop-base-start

RUN apt update
RUN apt install -y vim
RUN apt install -y dmidecode
RUN apt install -y procps
RUN apt install -y upower
RUN apt install -y python3.13-venv
WORKDIR /green_security_measurements

RUN python3 -m venv green_security_venv

COPY Scanner/requirements requirements
RUN green_security_venv/bin/pip3 install -r requirements/container_requirements.txt

COPY . .

# TODO: ENTER THE VENV
RUN source /green_security_measurements/green_security_venv/bin/activate

WORKDIR /green_security_measurements/Scanner

VOLUME ["/green_security_measurements"]

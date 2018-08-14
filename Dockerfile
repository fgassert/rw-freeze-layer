FROM python:3.6-alpine
MAINTAINER Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install requests sqlparse python-dateutil
RUN pip install git+https://github.com/fgassert/cartosql.py#master

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
WORKDIR /opt/$NAME/
COPY . .

RUN adduser -D $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "freeze"]

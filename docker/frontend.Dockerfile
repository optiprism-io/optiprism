FROM node as frontend
WORKDIR /app
RUN apt-get install -y git
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh git clone ssh://git@github.com/optiprism-io/frontend.git .
RUN npm i yarn --legacy-peer-deps
RUN npm i --legacy-peer-deps
RUN npm install -g vite
RUN vite build -d
FROM node as tracker
WORKDIR /app
RUN apt-get install -y git
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh git clone ssh://git@github.com/optiprism-io/optiprism-js.git .
RUN npm i yarn --legacy-peer-deps
RUN npm i --legacy-peer-deps
RUN npm install -g vite
RUN vite build -d
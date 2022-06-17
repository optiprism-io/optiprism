#build
FROM node:lts-alpine3.16 as build
WORKDIR /app
COPY ui/src src/
COPY ui/package.json .
COPY ui/tsconfig.json .
COPY ui/vite.config.ts .
COPY ui/yarn.lock .
COPY ui/index.html .
COPY ui/public .
RUN yarn install
EXPOSE 3000
CMD ["yarn", "run", "dev"]
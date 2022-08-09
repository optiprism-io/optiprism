#build
FROM node:lts-alpine3.16 as build
WORKDIR /app
COPY ui/src src/
COPY ui/package.json .
COPY ui/tsconfig.json .
COPY ui/vite.config.ts .
COPY ui/yarn.lock .
COPY ui/index.html .
COPY ui/public public/
ENV PORT 80
RUN yarn install
EXPOSE 80
CMD ["yarn", "run", "dev","--host"]
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
RUN yarn build
RUN ls -al
RUN ls -al /app/dist

#webserver
FROM nginx:stable-alpine
COPY --from=build /app/dist /usr/share/nginx/html
RUN ls -al /usr/share/nginx/html
COPY docker/ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
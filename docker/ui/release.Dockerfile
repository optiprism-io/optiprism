FROM node:lts-alpine3.16 as builder
WORKDIR /app
COPY ui/src src/
COPY ui/package.json .
COPY ui/tsconfig.json .
COPY ui/vite.config.ts .
COPY ui/yarn.lock .
COPY ui/index.html .
COPY ui/public public/
RUN yarn install
RUN yarn build

FROM nginx:stable-alpine AS runtime
COPY --from=builder /app/dist /usr/share/nginx/html
COPY docker/ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

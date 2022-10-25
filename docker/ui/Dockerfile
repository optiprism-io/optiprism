FROM node:lts-alpine3.16 as builder
WORKDIR /app
COPY frontend/src frontend/src
COPY frontend/index.html frontend/index.html
COPY frontend/public frontend/public
COPY package.json .
COPY tsconfig.json .
COPY vite.config.ts .
COPY yarn.lock .
RUN yarn install
RUN yarn build

FROM nginx:stable-alpine AS runtime
COPY --from=builder /app/frontend/dist /usr/share/nginx/html
COPY docker/ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

FROM node:lts-alpine3.16 as builder
WORKDIR /app
COPY frontend/src src/
COPY package.json .
COPY tsconfig.json .
COPY vite.config.ts .
COPY yarn.lock .
COPY index.html .
COPY public public/
RUN yarn install
RUN yarn build

FROM nginx:stable-alpine AS runtime
COPY --from=builder /app/dist /usr/share/nginx/html
COPY docker/ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

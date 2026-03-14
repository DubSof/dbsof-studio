FROM node:20-alpine AS builder
WORKDIR /app

COPY .yarn/releases/ .yarn/releases/
COPY .yarn/plugins/ .yarn/plugins/
COPY .yarnrc.yml yarn.lock ./
COPY package.json ./
COPY shared/auth/package.json        shared/auth/package.json
COPY shared/codeEditor/package.json  shared/codeEditor/package.json
COPY shared/common/package.json      shared/common/package.json
COPY shared/inspector/package.json   shared/inspector/package.json
COPY shared/lang-query/package.json  shared/lang-query/package.json
COPY shared/nodeGraph/package.json   shared/nodeGraph/package.json
COPY shared/platform/package.json    shared/platform/package.json
COPY shared/studio/package.json      shared/studio/package.json

RUN node .yarn/releases/yarn-3.2.0.cjs install

COPY . .
RUN node .yarn/releases/yarn-3.2.0.cjs build

FROM nginx:1.27-alpine AS runner
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/app.conf
COPY --from=builder /app/build /usr/share/nginx/html/ui
EXPOSE 3002
CMD ["nginx", "-g", "daemon off;"]

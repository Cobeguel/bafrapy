FROM node:22-alpine

RUN npm install -g directus-sync

WORKDIR /workspace

COPY directus-sync.config.js .

ENTRYPOINT ["directus-sync"]
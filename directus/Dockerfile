FROM directus/directus:11.15.1

USER root
RUN corepack enable
USER node
RUN pnpm install directus-extension-sync
RUN pnpm install directus-sync
USER root

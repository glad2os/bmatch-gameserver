FROM node:24-alpine

ENV NODE_ENV=production
WORKDIR /usr/src/app

COPY package.json package-lock.json ./
# npm is only needed to install dependencies; drop it so its bundled packages are not shipped.
RUN npm ci --omit=dev && npm cache clean --force \
    && rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
        /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack

COPY --chown=node:node . .

USER node

EXPOSE 8004 8005
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD ["node", "-e", "fetch('http://localhost:8005/health').then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1))"]

CMD ["node", "app.js"]

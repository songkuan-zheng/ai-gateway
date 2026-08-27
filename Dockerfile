FROM denoland/deno:bin AS deno-bin

FROM node:22-bookworm-slim AS build

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM node:22-bookworm-slim AS runtime

WORKDIR /app

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000 \
    HOME=/tmp \
    DENO_DIR=/tmp/deno \
    RAW_TRACE_SPOOL_DIR=/data/gateway/raw-trace \
    AGENT_STORAGE_DIR=/data/gateway/agent

COPY --from=deno-bin /deno /usr/local/bin/deno

COPY package.json package-lock.json ./
RUN npm ci --omit=dev \
  && npm cache clean --force \
  && mkdir -p /data/gateway/raw-trace /data/gateway/agent \
  && chown -R node:node /app /data/gateway

COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node bin ./bin
COPY --chown=node:node docs ./docs
COPY --chown=node:node README.md README.zh-CN.md LICENSE ./

USER node

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/ready').then((r)=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "dist/index.js"]

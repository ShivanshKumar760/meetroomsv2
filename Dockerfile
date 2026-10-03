# Dockerfile
FROM node:20-slim

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

COPY --chown=node:node . .
USER node

EXPOSE 3000
CMD ["node", "server.js"]
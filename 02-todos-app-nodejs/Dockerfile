# syntax=docker/dockerfile:1

FROM node:18-alpine
RUN apk add python3 make
WORKDIR /app
COPY . .
RUN yarn install --production
CMD ["node", "src/index.js"]
EXPOSE 3000
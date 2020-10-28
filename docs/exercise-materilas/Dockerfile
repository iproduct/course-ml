FROM node:12-alpine
WORKDIR /app
COPY package.json yarn.lock ./
RUN yarn install --production
COPY . .
# COPY . .
# RUN yarn install --production
CMD ["node", "src/index.js"]
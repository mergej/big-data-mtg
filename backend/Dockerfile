FROM node:latest

WORKDIR /app

COPY package*.json /app

RUN npm install

COPY . /app

ENV ENVIRONMENT=dev

CMD ["npm", "run", "start"]
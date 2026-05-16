FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY gateway.js archiver.js start.sh ./

RUN chmod +x start.sh

EXPOSE 3000

CMD ["sh", "start.sh"]

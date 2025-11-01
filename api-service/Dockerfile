FROM node:20-alpine

RUN apk add --no-cache curl openssl

WORKDIR /app

COPY package*.json ./

COPY prisma ./prisma/

RUN npm ci --only=production && npm cache clean --force

RUN npx prisma generate

COPY . .

RUN npm run build

EXPOSE 3000

CMD ["sh", "-c", "npx prisma migrate deploy && node dist/index.js"]
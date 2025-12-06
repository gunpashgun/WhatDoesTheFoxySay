FROM apify/actor-node-playwright-chrome:20

WORKDIR /usr/src/app

COPY --chown=myuser:myuser package.json ./

USER root
RUN npm install --omit=dev --quiet
USER myuser

COPY --chown=myuser:myuser . ./

CMD ["node", "main.js"]

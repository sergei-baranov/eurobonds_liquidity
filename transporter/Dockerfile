FROM openjdk:8
COPY ./target /usr/bin/app
WORKDIR /usr/bin/app
ENV CBONDS_USER_LOGIN Sergey Baranov
ENV CBONDS_USER_PASSWORD 123
ENTRYPOINT java -jar transporter-1.0-SNAPSHOT-uber.jar com.sergei_baranov.eurobonds_liquidity.transporter.DataTransporter "$CBONDS_USER_LOGIN" "$CBONDS_USER_PASSWORD"

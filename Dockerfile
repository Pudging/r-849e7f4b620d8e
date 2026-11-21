FROM gcc:13 AS build
WORKDIR /src
COPY main.cpp csv.hpp keys.hpp parquet.hpp ./
RUN g++ -std=c++17 -O3 -DNDEBUG -pthread -static -s main.cpp -o /engine

FROM scratch
COPY --from=build /engine /engine
USER 1000:1000
ENTRYPOINT ["/engine"]
